/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Modified work, Copyright (c) 2016 Istvan Bartha


 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package tasks.fileservice.s3

import tasks.util._
import tasks.util.config._
import tasks.util.eq._

import java.io.File

import cats.effect.kernel.Resource
import cats.effect.IO
import fs2.{Stream, Pipe}
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import tasks.fileservice._

class S3Storage(
    bucketName: String,
    folderPrefix: String,
    sse: String,
    cannedAcls: Seq[String],
    grantFullControl: Seq[String],
    uploadParallelism: Int,
    s3: tasks.fileservice.s3.S3
)(implicit
    config: TasksConfig
) extends ManagedFileStorage {

  override def toString = s"S3Storage(bucket=$bucketName, prefix=$folderPrefix)"

  def sharedFolder(prefix: Seq[String]): IO[Option[File]] = IO.pure(None)

  def contains(
      path: ManagedFilePath,
      retrieveSizeAndHash: Boolean
  ): IO[Option[SharedFile]] =
    s3
      .getObjectMetadata(bucketName, assembleName(path))
      .map { meta =>
        meta.map { meta =>
          val (size1, hash1) = getLengthAndHash(meta)
          SharedFileHelper.create(size1, hash1, path)
        }
      }

  def contains(path: ManagedFilePath, size: Long, hash: Int): IO[Boolean] =
    s3
      .getObjectMetadata(bucketName, assembleName(path))
      .map {
        case Some(meta) =>
          if (size < 0) true
          else {
            val (size1, hash1) = getLengthAndHash(meta)
            size1 === size && (config.skipContentHashVerificationAfterCache || hash === hash1)
          }
        case None => false
      }

  private def getLengthAndHash(meta: HeadObjectResponse) = {
    val size1: Long = meta.contentLength()
    val hash1: Int = meta.eTag().hashCode()
    (size1, hash1)
  }

  private def assembleName(path: ManagedFilePath) =
    ((if (folderPrefix != "") folderPrefix + "/" else "") +: path.pathElements)
      .filterNot(_ == "/")
      .map(x => if (x.startsWith("/")) x.drop(1) else x)
      .map(x => if (x.endsWith("/")) x.dropRight(1) else x)
      .mkString("/")

  def sink(
      path: ProposedManagedFilePath
  ): Pipe[IO, Byte, (Long, Int, ManagedFilePath)] = { (in: Stream[IO, Byte]) =>
    val assembledPath = assembleName(path.toManaged)
    Stream.eval(
      in.through(
        s3.uploadFileMultipart(
          bucket = bucketName,
          key = assembledPath,
          partSize = 5,
          multiPartConcurrency = uploadParallelism,
          cannedAcl = cannedAcls.toList,
          serverSideEncryption = Some(sse),
          grantFullControl = grantFullControl.toList
        )
      ).compile
        .drain
        .flatMap { _ =>
          s3
            .getObjectMetadata(bucketName, assembledPath)
            .map {
              case None =>
                throw new RuntimeException(
                  s"S3: Reading back just uploaded object failed. $path"
                )
              case Some(meta) =>
                val (size1, hash1) = getLengthAndHash(meta)
                (size1, hash1, path.toManaged)
            }

        }
    )
  }

  def stream(
      path: ManagedFilePath,
      fromOffset: Long
  ): Stream[IO, Byte] = {
    val assembledPath = assembleName(path)

    val length = s3
      .getObjectMetadata(bucketName, assembledPath)
      .map { m =>
        if (m.isEmpty)
          throw new RuntimeException(s"S3: File does not exists $assembledPath")
        val (size1, _) = getLengthAndHash(m.get)
        size1
      }

    Stream.force(length.map { length =>
      if (length > 1024 * 1024 * 10)
        s3.readFileMultipart(bucketName, assembledPath, 1)
      else s3.readFile(bucketName, assembledPath)
    })

  }

  def exportFile(path: ManagedFilePath): Resource[IO, File] =
    Resource.make {

      val file = TempFile.createTempFile("")

      val assembledPath = assembleName(path)

      val upload = s3
        .readFile(bucketName, assembledPath)
        .through(
          fs2.io.file
            .Files[IO]
            .writeAll(fs2.io.file.Path.fromNioPath(file.toPath()))
        )
        .compile
        .drain

      val verify = s3
        .getObjectMetadata(bucketName, assembledPath)

      (upload *> verify).map { metadata =>
        if (metadata.isEmpty) {
          throw new RuntimeException("S3: File does not exists")
        }

        val (size1, _) = getLengthAndHash(metadata.get)
        if (size1 !== file.length)
          throw new RuntimeException("S3: Downloaded file length != metadata")

        file
      }

    }(file => IO(file.delete))

  def uri(mp: ManagedFilePath): IO[tasks.util.Uri] =
    IO.pure(tasks.util.Uri("s3://" + bucketName + "/" + assembleName(mp)))

  def delete(mp: ManagedFilePath, size: Long, hash: Int) =
    IO.pure(false)

}
