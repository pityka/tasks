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

class S3Storage(bucketName: String, folderPrefix: String, s3stream: S3Stream)(
    implicit mat: ActorMaterializer,
    as: ActorSystem,
    ec: ExecutionContext)
    extends ManagedFileStorage {

  val log = akka.event.Logging(as.eventStream, getClass)

  val putObjectParams = {
    val sse = config.global.s3ServerSideEncryption
    val cannedAcls = config.global.s3CannedAcl
    val grantFullControl = config.global.s3GrantFullControl

    val rq = grantFullControl.foldLeft(
        cannedAcls.foldLeft(PostObjectRequest.default)((rq, acl) =>
              rq.cannedAcl(acl)))((rq, acl) =>
          rq.grantFullControl(acl._1, acl._2))
    if (sse) rq.serverSideEncryption else rq
  }

  def list(pattern: String): List[SharedFile] = ???

  def contains(path: ManagedFilePath, size: Long, hash: Int): Future[Boolean] =
    s3stream.getMetadata(S3Location(bucketName, assembleName(path))).map {
      metadata =>
        if (size < 0 && metadata.response.status.intValue == 200) true
        else {
          val (size1, hash1) = getLengthAndHash(metadata)
          metadata.response.status.intValue == 200 && size1 === size && (config.global.skipContentHashVerificationAfterCache || hash === hash1)
        }
    } recover {
      case x: Exception =>
        log.debug("This might be an error, or likely a missing file. {}", x)
        false
    }

  def getLengthAndHash(metadata: ObjectMetadata): (Long, Int) =
    metadata.contentLength.get -> metadata.eTag.get.hashCode

  private def assembleName(path: ManagedFilePath) =
    ((if (folderPrefix != "") folderPrefix + "/" else "") +: path.pathElements)
      .filterNot(_ == "/")
      .map(x => if (x.startsWith("/")) x.drop(1) else x)
      .map(x => if (x.endsWith("/")) x.dropRight(1) else x)
      .mkString("/")

  def importSource(s: Source[ByteString, _], path: ProposedManagedFilePath)(
      implicit am: Materializer): Future[(Long, Int, ManagedFilePath)] = {
    val managed = path.toManaged
    val key = assembleName(managed)
    val s3loc = S3Location(bucketName, key)
    val sink = s3stream.multipartUpload(s3loc, params = putObjectParams)
    val future = s.runWith(sink)

    future.flatMap(_ => s3stream.getMetadata(s3loc)).map { metadata =>
      val (size1, hash1) = getLengthAndHash(metadata)

      (size1, hash1, managed)
    }
  }

  def importFile(f: File, path: ProposedManagedFilePath)
    : Future[(Long, Int, File, ManagedFilePath)] = {
    val managed = path.toManaged
    val key = assembleName(managed)
    val s3loc = S3Location(bucketName, key)

    val sink = s3stream.multipartUpload(s3loc, params = putObjectParams)

    FileIO.fromPath(f.toPath).runWith(sink).flatMap { _ =>
      s3stream.getMetadata(s3loc).map { metadata =>
        val (size1, hash1) = getLengthAndHash(metadata)

        if (size1 !== f.length)
          throw new RuntimeException("S3: Uploaded file length != on disk")

        (size1, hash1, f, managed)
      }
    }

  }

  def createStream(path: ManagedFilePath): Try[InputStream] =
    Try(
        s3stream
          .getData(S3Location(bucketName, assembleName(path)))
          .runWith(StreamConverters.asInputStream()))

  def createSource(path: ManagedFilePath): Source[ByteString, _] =
    s3stream.getData(S3Location(bucketName, assembleName(path)))

  def exportFile(path: ManagedFilePath): Future[File] = {
    val file = TempFile.createTempFile("")
    val s3loc = S3Location(bucketName, assembleName(path))

    val f1 = s3stream.getData(s3loc).runWith(FileIO.toPath(file.toPath))

    f1.flatMap(_ => s3stream.getMetadata(s3loc)).map { metadata =>
      val (size1, hash1) = getLengthAndHash(metadata)
      if (size1 !== file.length)
        throw new RuntimeException("S3: Downloaded file length != metadata")

      file
    }

  }

  def uri(mp: ManagedFilePath): tasks.util.Uri =
    tasks.util.Uri("s3://" + bucketName + "/" + assembleName(mp))

}
