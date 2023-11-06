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

import com.google.common.hash._

import java.io.{File, InputStream}

import tasks.util._
import tasks.util.config.TasksConfig
import tasks.util.eq._
import cats.effect.kernel.Resource
import cats.effect.IO
import fs2.Stream

object FileStorage {
  def getContentHash(is: InputStream): Int = {
    val checkedSize = 1024 * 256
    val buffer = Array.fill[Byte](checkedSize)(0)
    val his = new HashingInputStream(Hashing.crc32c, is)

    com.google.common.io.ByteStreams.read(his, buffer, 0, buffer.size)
    his.hash.asInt
  }
}

class RemoteFileStorage(implicit
    streamHelper: StreamHelper,
    config: TasksConfig
) {


  def uri(mp: RemoteFilePath): Uri = mp.uri

  def stream(
      path: RemoteFilePath,
      fromOffset: Long
  ): Stream[IO, Byte] =
    streamHelper.createStream(uri(path), fromOffset)

  def getSizeAndHash(path: RemoteFilePath): IO[(Long, Int)] =
    path.uri.scheme match {
      case "file" =>
        IO.interruptible {
          val file = new File(path.uri.path)
          val hash = openFileInputStream(file) { is =>
            FileStorage.getContentHash(is)
          }
          (file.length, hash)
        }
      case "s3" | "http" | "https" =>
        streamHelper
          .getContentLengthAndETag(uri(path))
          .map { case (size, etag) =>
            (
              size.getOrElse(
                throw new RuntimeException(s"Size can't retrieved for $path")
              ),
              etag.map(_.hashCode).getOrElse(-1)
            )
          }
    }

  def contains(path: RemoteFilePath, size: Long, hash: Int): IO[Boolean] =
    getSizeAndHash(path)
      .map { case (size1, hash1) =>
        size < 0 || (size1 === size && (config.skipContentHashVerificationAfterCache || hash === hash1))
      }
      .handleError { case e =>
        scribe.debug("Exception while looking up remote file. {}", e)
        false
      }

  def exportFile(path: RemoteFilePath): Resource[IO, File] = {
    val localFile = path.uri.scheme == "file" && new File(
      path.uri.path.toString
    ).canRead
    if (localFile) Resource.pure(new File(path.uri.path.toString))
    else {
      Resource.make({
        val file = TempFile.createTempFile("")
        stream(path, fromOffset = 0L)
          .through(
            fs2.io.file
              .Files[IO]
              .writeAll(fs2.io.file.Path.fromNioPath(file.toPath))
          )
          .compile
          .drain
          .map(_ => file)

      })(file => IO(file.delete()))
    }

  }

}

trait ManagedFileStorage {


  def uri(mp: ManagedFilePath): IO[Uri]

  def stream(
      path: ManagedFilePath,
      fromOffset: Long
  ): Stream[IO, Byte]

  /* If size < 0 then it must not check the size and the hash
   *  but must return true iff the file is readable
   */
  def contains(path: ManagedFilePath, size: Long, hash: Int): IO[Boolean]

  def contains(
      path: ManagedFilePath,
      retrieveSizeAndHash: Boolean
  ): IO[Option[SharedFile]]

  def importFile(
      f: File,
      path: ProposedManagedFilePath
  ): IO[(Long, Int, ManagedFilePath)] =
    tasks.util.retryIO(s"upload to $path")(importFile1(f, path), 4)(scribe.Logger[ManagedFileStorage])

  private def importFile1(
      f: File,
      path: ProposedManagedFilePath
  ): IO[(Long, Int, ManagedFilePath)] = IO.unit.flatMap { _ =>
    fs2.io.file
      .Files[IO]
      .readAll(fs2.io.file.Path.fromNioPath(f.toPath))
      .through(sink(path))
      .compile
      .lastOrError
  }

  def sink(
      path: ProposedManagedFilePath
  ): fs2.Pipe[IO, Byte, (Long, Int, ManagedFilePath)]

  def exportFile(path: ManagedFilePath): Resource[IO, File]

  def sharedFolder(prefix: Seq[String]): IO[Option[File]]

  def delete(
      path: ManagedFilePath,
      expectedSize: Long,
      expectedHash: Int
  ): IO[Boolean]

}
