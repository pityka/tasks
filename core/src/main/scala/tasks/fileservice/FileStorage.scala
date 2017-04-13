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
import akka.pattern.ask
import akka.pattern.pipe
import akka.stream.scaladsl._
import akka.stream._
import akka.util._

import com.google.common.hash._

import scala.concurrent.duration._
import scala.concurrent._
import scala.util._

import java.lang.Class
import java.io.{File, InputStream, FileInputStream, BufferedInputStream}
import java.util.concurrent.{TimeUnit, ScheduledFuture}
import java.nio.channels.{WritableByteChannel, ReadableByteChannel}

import tasks.util._
import tasks.util.eq._
import tasks.caching._
import tasks.queue._

object FileStorage {
  def getContentHash(is: InputStream): Int = {
    val checkedSize = 1024 * 256
    val buffer = Array.fill[Byte](checkedSize)(0)
    val his = new HashingInputStream(Hashing.crc32c, is)

    com.google.common.io.ByteStreams.read(his, buffer, 0, buffer.size)
    his.hash.asInt
  }
}

class RemoteFileStorage(implicit mat: ActorMaterializer,
                        ec: ExecutionContext,
                        streamHelper: StreamHelper) {

  def uri(mp: RemoteFilePath): Uri = mp.uri

  def createSource(path: RemoteFilePath): Source[ByteString, _] =
    streamHelper.createSource(uri(path).akka)

  def getSizeAndHash(path: RemoteFilePath): Future[(Long, Int)] =
    streamHelper.getContentLength(uri(path).akka).map { size1 =>
      val is = streamHelper
        .createSource(uri(path).akka)
        .runWith(StreamConverters.asInputStream())
      val hash1 = FileStorage.getContentHash(is)
      is.close
      (size1.toLong, hash1)
    }

  def contains(path: RemoteFilePath, size: Long, hash: Int): Future[Boolean] =
    getSizeAndHash(path).map {
      case (size1, hash1) =>
        size1 === size && (tasks.util.config.global.skipContentHashVerificationAfterCache || hash === hash1)
    }.recover { case _ => false }

  def exportFile(path: RemoteFilePath): Future[File] = {
    val localFile = path.uri.akka.scheme == "file" && new File(
          path.uri.akka.path.toString).canRead
    if (localFile) Future.successful(new File(path.uri.akka.path.toString))
    else {
      val file = TempFile.createTempFile("")
      createSource(path).runWith(FileIO.toPath(file.toPath)).map(_ => file)
    }

  }

  def createStream(path: RemoteFilePath): Try[InputStream] = Try(
      createSource(path).runWith(StreamConverters.asInputStream())
  )

}

trait ManagedFileStorage {

  def uri(mp: ManagedFilePath): Uri

  def createSource(path: ManagedFilePath): Source[ByteString, _]

  def contains(path: ManagedFilePath, size: Long, hash: Int): Boolean

  def importFile(
      f: File,
      path: ProposedManagedFilePath): Try[(Long, Int, File, ManagedFilePath)]

  def importSource(s: Source[ByteString, _], path: ProposedManagedFilePath)(
      implicit am: Materializer): Future[(Long, Int, ManagedFilePath)]

  def exportFile(path: ManagedFilePath): Future[File]

  def createStream(path: ManagedFilePath): Try[InputStream]

  def list(regexp: String): List[SharedFile]

}
