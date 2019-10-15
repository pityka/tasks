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

import akka.actor._
import akka.stream.scaladsl._
import akka.stream._
import akka.util._

import com.google.common.hash._

import scala.concurrent._

import java.io.{File, InputStream}

import tasks.util._
import tasks.util.config.TasksConfig
import tasks.util.eq._

object FileStorage {
  def getContentHash(is: InputStream): Int = {
    val checkedSize = 1024 * 256
    val buffer = Array.fill[Byte](checkedSize)(0)
    val his = new HashingInputStream(Hashing.crc32c, is)

    com.google.common.io.ByteStreams.read(his, buffer, 0, buffer.size)
    his.hash.asInt
  }
}

class RemoteFileStorage(
    implicit mat: Materializer,
    ec: ExecutionContext,
    streamHelper: StreamHelper,
    as: ActorSystem,
    config: TasksConfig
) {

  val log = akka.event.Logging(as.eventStream, "remote-storage")

  def uri(mp: RemoteFilePath): Uri = mp.uri

  def createSource(
      path: RemoteFilePath,
      fromOffset: Long
  ): Source[ByteString, _] =
    streamHelper.createSource(uri(path), fromOffset)

  def getSizeAndHash(path: RemoteFilePath): Future[(Long, Int)] =
    path.uri.scheme match {
      case "file" =>
        val file = new File(path.uri.path)
        openFileInputStream(file) { is =>
          val hash = FileStorage.getContentHash(is)
          Future.successful((file.length, hash))
        }
      case "s3" | "http" | "https" =>
        streamHelper.getContentLengthAndETag(uri(path)).map {
          case (size, etag) =>
            (
              size.getOrElse(
                throw new RuntimeException(s"Size can't retrieved for $path")
              ),
              etag.map(_.hashCode).getOrElse(-1)
            )
        }
    }

  def contains(path: RemoteFilePath, size: Long, hash: Int): Future[Boolean] =
    getSizeAndHash(path)
      .map {
        case (size1, hash1) =>
          size < 0 || (size1 === size && (config.skipContentHashVerificationAfterCache || hash === hash1))
      }
      .recover {
        case e =>
          log.debug("Exception while looking up remote file. {}", e)
          false
      }

  def exportFile(path: RemoteFilePath): Future[File] = {
    val localFile = path.uri.akka.scheme == "file" && new File(
      path.uri.akka.path.toString
    ).canRead
    if (localFile) Future.successful(new File(path.uri.akka.path.toString))
    else {
      val file = TempFile.createTempFile("")
      createSource(path, fromOffset = 0L)
        .runWith(FileIO.toPath(file.toPath))
        .map(_ => file)
    }

  }

}

trait ManagedFileStorage {

  def uri(mp: ManagedFilePath): Uri

  def createSource(
      path: ManagedFilePath,
      fromOffset: Long
  ): Source[ByteString, _]

  /* If size < 0 then it must not check the size and the hash
   *  but must return true iff the file is readable
   */
  def contains(path: ManagedFilePath, size: Long, hash: Int): Future[Boolean]

  def contains(
      path: ManagedFilePath,
      retrieveSizeAndHash: Boolean
  ): Future[Option[SharedFile]]

  def importFile(
      f: File,
      path: ProposedManagedFilePath
  ): Future[(Long, Int, File, ManagedFilePath)]

  def sink(
      path: ProposedManagedFilePath
  ): Sink[ByteString, Future[(Long, Int, ManagedFilePath)]]

  def exportFile(path: ManagedFilePath): Future[File]

  def list(regexp: String): List[SharedFile]

  def sharedFolder(prefix: Seq[String]): Option[File]

  def delete(
      path: ManagedFilePath,
      expectedSize: Long,
      expectedHash: Int
  ): Future[Boolean]

}
