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

import akka.actor.{Actor, PoisonPill, ActorRef, Props, Cancellable}
import scala.concurrent.duration._
import java.util.concurrent.{TimeUnit, ScheduledFuture}
import java.net.InetSocketAddress
import akka.actor.Actor._
import akka.event.LoggingAdapter
import scala.util._

import tasks.deploy._
import tasks.shared._
import tasks.util._
import tasks.util.eq._
import tasks.fileservice._

import com.amazonaws.AmazonServiceException
import java.net.URL

import collection.JavaConversions._
import java.io.{File, InputStream}

import tasks.util.S3Helpers
import akka.stream._
import akka.actor._
import akka.util._
import akka.http.scaladsl.model._
import com.bluelabs.s3stream._
import com.bluelabs.akkaaws._
import akka.stream.scaladsl._
import scala.concurrent._
import scala.concurrent.duration._

class S3Storage(bucketName: String, folderPrefix: String) extends FileStorage {

  def list(pattern: String): List[SharedFile] = ???

  def centralized = true

  @transient private var _system = ActorSystem("s3storage");

  implicit def system = {
    if (_system == null) {
      _system = ActorSystem("s3storage");
    }
    _system
  }

  implicit def ec = system.dispatcher

  @transient private var _mat = ActorMaterializer()

  implicit def mat = {
    if (_mat == null) {
      _mat = ActorMaterializer()
    }
    _mat
  }

  @transient private var s3stream = new S3Stream(S3Helpers.credentials)

  override def close = _system.shutdown

  def contains(path: ManagedFilePath, size: Long, hash: Int): Boolean = {
    val tr = retry(5) {
      try {
        val metadata = Await.result(
            s3stream.getMetadata(S3Location(bucketName, assembleName(path))),
            atMost = 5 seconds)
        if (size < 0) true
        else {
          val (size1, hash1) = getLengthAndHash(metadata)
          size1 === size && (config.global.skipContentHashVerificationAfterCache || hash === hash1)
        }
      } catch {
        case x: AmazonServiceException => false
      }
    }

    tr.failed.foreach(println)

    tr.getOrElse(false)
  }

  def getLengthAndHash(metadata: HttpResponse): (Long, Int) =
    metadata
      .header[akka.http.scaladsl.model.headers.`Content-Length`]
      .get
      .length -> metadata
      .header[akka.http.scaladsl.model.headers.`ETag`]
      .get
      .value
      .hashCode

  private def assembleName(path: ManagedFilePath) =
    ((if (folderPrefix != "") folderPrefix + "/" else "") +: path.pathElements)
      .filterNot(_ == "/")
      .map(x => if (x.startsWith("/")) x.drop(1) else x)
      .map(x => if (x.endsWith("/")) x.dropRight(1) else x)
      .mkString("/")

  def importSource(s: Source[ByteString, _], path: ProposedManagedFilePath)(
      implicit am: Materializer): Try[(Long, Int, ManagedFilePath)] = Try {
    val managed = path.toManaged
    val key = assembleName(managed)
    val s3loc = S3Location(bucketName, key)
    val sink = s3stream.multipartUpload(s3loc, serverSideEncryption = true)
    val future = s.runWith(sink)

    val metadata =
      Await.result(future.flatMap(_ => s3stream.getMetadata(s3loc)),
                   atMost = 24 hours)

    val (size1, hash1) = getLengthAndHash(metadata)

    // if (size1 !== f.length)
    //   throw new RuntimeException("S3: Uploaded file length != on disk")

    (size1, hash1, managed)
  }

  def importFile(f: File, path: ProposedManagedFilePath)
    : Try[(Long, Int, File, ManagedFilePath)] = {
    val managed = path.toManaged
    val key = assembleName(managed)
    val s3loc = S3Location(bucketName, key)
    retry(5) {

      val sink = s3stream.multipartUpload(s3loc, serverSideEncryption = true)

      val future = FileIO.fromPath(f.toPath).runWith(sink)

      val metadata =
        Await.result(future.flatMap(_ => s3stream.getMetadata(s3loc)),
                     atMost = 24 hours)

      val (size1, hash1) = getLengthAndHash(metadata)

      if (size1 !== f.length)
        throw new RuntimeException("S3: Uploaded file length != on disk")

      (size1, hash1, f, managed)

    }

  }

  def openStream(path: ManagedFilePath): Try[InputStream] =
    Try(
        s3stream
          .download(S3Location(bucketName, assembleName(path)))
          .runWith(StreamConverters.asInputStream()))

  def createSource(path: ManagedFilePath): Source[ByteString, _] =
    s3stream.download(S3Location(bucketName, assembleName(path)))

  def exportFile(path: ManagedFilePath): Try[File] = {
    val file = TempFile.createTempFile("")
    val s3loc = S3Location(bucketName, assembleName(path))
    retry(5) {
      val f1 = s3stream.download(s3loc).runWith(FileIO.toPath(file.toPath))

      val metadata = Await.result(f1.flatMap(_ => s3stream.getMetadata(s3loc)),
                                  atMost = 24 hours)

      val (size1, hash1) = getLengthAndHash(metadata)
      if (size1 !== file.length)
        throw new RuntimeException("S3: Downloaded file length != metadata")

      file
    }
  }

  def url(mp: ManagedFilePath): URL =
    new URL("s3", bucketName, assembleName(mp))

}
