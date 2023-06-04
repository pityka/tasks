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

package tasks.fileservice

import tasks.util._
import tasks.util.config._
import tasks.util.eq._

import java.io.File

import akka.stream._
import akka.actor._
import akka.util._
import akka.stream.scaladsl._
import scala.concurrent._
import akka.stream.alpakka.s3.headers.CannedAcl
import akka.stream.alpakka.s3.headers.ServerSideEncryption
import akka.stream.alpakka.s3.ObjectMetadata
import akka.stream.alpakka.s3.scaladsl.S3
import cats.effect.kernel.Resource
import cats.effect.IO

class S3Storage(
    bucketName: String,
    folderPrefix: String
)(implicit
    mat: Materializer,
    as: ActorSystem,
    ec: ExecutionContext,
    config: TasksConfig
) extends ManagedFileStorage {

  override def toString = s"S3Storage(bucket=$bucketName, prefix=$folderPrefix)"

  implicit val log = akka.event.Logging(as.eventStream, getClass)

  val s3Headers = {
    val sse = config.s3ServerSideEncryption
    val cannedAcls = config.s3CannedAcl
    val grantFullControl = config.s3GrantFullControl

    val rq = grantFullControl.foldLeft(
      cannedAcls
        .foldLeft(akka.stream.alpakka.s3.S3Headers.empty)((rq, acl) =>
          rq.withCannedAcl(acl match {
            case "authenticated-read"        => CannedAcl.AuthenticatedRead
            case "aws-exec-read"             => CannedAcl.AwsExecRead
            case "bucket-owner-full-control" => CannedAcl.BucketOwnerFullControl
            case "bucket-owner-read"         => CannedAcl.BucketOwnerRead
            case "private"                   => CannedAcl.Private
            case "public-read"               => CannedAcl.PublicRead
            case "public-read-write"         => CannedAcl.PublicReadWrite
          })
        )
    )((rq, acl) =>
      rq.withCustomHeaders(
        Map(("x-amz-grant-full-control", acl._1 + "=" + acl._2))
      )
    )
    if (sse) rq.withServerSideEncryption(ServerSideEncryption.aes256()) else rq
  }

  def sharedFolder(prefix: Seq[String]): Future[Option[File]] = Future.successful(None)


  def contains(
      path: ManagedFilePath,
      retrieveSizeAndHash: Boolean
  ): Future[Option[SharedFile]] =
    S3
      .getObjectMetadata(bucketName, assembleName(path))
      .map { meta =>
        meta.map { meta =>
          val (size1, hash1) = getLengthAndHash(meta)
          SharedFileHelper.create(size1, hash1, path)
        }
      }
      .runWith(Sink.head)

  def contains(path: ManagedFilePath, size: Long, hash: Int): Future[Boolean] =
    S3
      .getObjectMetadata(bucketName, assembleName(path))
      .map {
        case Some(metadata) =>
          if (size < 0) true
          else {
            val (size1, hash1) = getLengthAndHash(metadata)
            size1 === size && (config.skipContentHashVerificationAfterCache || hash === hash1)
          }
        case None => false
      }
      .runWith(Sink.head)

  def getLengthAndHash(metadata: ObjectMetadata): (Long, Int) =
    metadata.contentLength -> metadata.eTag.get.hashCode

  private def assembleName(path: ManagedFilePath) =
    ((if (folderPrefix != "") folderPrefix + "/" else "") +: path.pathElements)
      .filterNot(_ == "/")
      .map(x => if (x.startsWith("/")) x.drop(1) else x)
      .map(x => if (x.endsWith("/")) x.dropRight(1) else x)
      .mkString("/")

  def sink(
      path: ProposedManagedFilePath
  ): Sink[ByteString, Future[(Long, Int, ManagedFilePath)]] = {
    val managed = path.toManaged
    val key = assembleName(managed)

    val sink = S3.multipartUploadWithHeaders(
      bucket = bucketName,
      key = key,
      s3Headers = s3Headers
    )

    sink.mapMaterializedValue(
      _.flatMap(_ =>
        S3
          .getObjectMetadata(bucketName, key)
          .runWith(Sink.head)
      ).map {
        case None =>
          throw new RuntimeException(
            "Failed to fetch metadata of just uploaded file"
          )

        case Some(metadata) =>
          val (size1, hash1) = getLengthAndHash(metadata)
          (size1, hash1, managed)
      }
    )

  }

  def importFile(
      f: File,
      path: ProposedManagedFilePath
  ): Future[(Long, Int, ManagedFilePath)] =
    tasks.util.retryFuture(s"upload to $path")(importFile1(f, path), 4)

  def importFile1(
      f: File,
      path: ProposedManagedFilePath
  ): Future[(Long, Int, ManagedFilePath)] = {
    val managed = path.toManaged
    val key = assembleName(managed)

    val sink = S3.multipartUploadWithHeaders(
      bucket = bucketName,
      key = key,
      s3Headers = s3Headers
    )

    FileIO.fromPath(f.toPath).runWith(sink).flatMap { _ =>
      S3
        .getObjectMetadata(bucketName, key)
        .runWith(Sink.head)
        .map {
          case None =>
            throw new RuntimeException(
              "Failed to fetch metadata of just uploaded file"
            )
          case Some(metadata) =>
            val (size1, hash1) = getLengthAndHash(metadata)

            if (size1 !== f.length)
              throw new RuntimeException("S3: Uploaded file length != on disk")

            (size1, hash1, managed)
        }
    }

  }

  def createSource(
      path: ManagedFilePath,
      fromOffset: Long
  ): Source[ByteString, _] = {
    assert(fromOffset == 0L, "seeking into s3 not implemented ")
    S3
      .download(bucketName, assembleName(path))
      .flatMapConcat {
        case None                  => Source.empty
        case Some((dataSource, _)) => dataSource
      }
  }

  def exportFile(path: ManagedFilePath): Resource[IO, File] =
    Resource.make {
      IO.fromFuture(IO {
        val file = TempFile.createTempFile("")

        val assembledPath = assembleName(path)

        val f1 = S3
          .download(bucketName, assembledPath)
          .flatMapConcat {
            case None                  => Source.empty
            case Some((dataSource, _)) => dataSource
          }
          .runWith(FileIO.toPath(file.toPath))

        val f2 = S3
          .getObjectMetadata(bucketName, assembledPath)
          .flatMapConcat {
            case None        => Source.empty
            case Some(value) => Source.single(value)
          }
          .runWith(Sink.headOption)

        f1.flatMap(_ => f2).map { metadata =>
          if (metadata.isEmpty) {
            throw new RuntimeException("S3: File does not exists")
          }

          val (size1, _) = getLengthAndHash(metadata.get)
          if (size1 !== file.length)
            throw new RuntimeException("S3: Downloaded file length != metadata")

          file
        }
      })
    }(file => IO(file.delete))

  def uri(mp: ManagedFilePath): Future[tasks.util.Uri] =
    Future.successful(tasks.util.Uri("s3://" + bucketName + "/" + assembleName(mp)))

  def delete(mp: ManagedFilePath, size: Long, hash: Int) =
    Future.successful(false)

}
