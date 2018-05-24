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
import com.bluelabs.s3stream._
import akka.stream.scaladsl._
import scala.concurrent._

class S3Storage(bucketName: String,
                folderPrefix: String,
                s3stream: S3ClientSupport)(implicit mat: Materializer,
                                           as: ActorSystem,
                                           ec: ExecutionContext,
                                           config: TasksConfig)
    extends ManagedFileStorage {

  val log = akka.event.Logging(as.eventStream, getClass)

  val putObjectParams = {
    val sse = config.s3ServerSideEncryption
    val cannedAcls = config.s3CannedAcl
    val grantFullControl = config.s3GrantFullControl

    val rq = grantFullControl.foldLeft(
      cannedAcls.foldLeft(PostObjectRequest.default)((rq, acl) =>
        rq.cannedAcl(acl)))((rq, acl) => rq.grantFullControl(acl._1, acl._2))
    if (sse) rq.serverSideEncryption else rq
  }

  def list(pattern: String): List[SharedFile] = ???

  def contains(path: ManagedFilePath, size: Long, hash: Int): Future[Boolean] =
    s3stream.getMetadata(S3Location(bucketName, assembleName(path))).map {
      metadata =>
        if (size < 0 && metadata.response.status.intValue == 200) true
        else {
          val (size1, hash1) = getLengthAndHash(metadata)
          metadata.response.status.intValue == 200 && size1 === size && (config.skipContentHashVerificationAfterCache || hash === hash1)
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
      implicit mat: Materializer): Future[(Long, Int, ManagedFilePath)] = {
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
    : Future[(Long, Int, File, ManagedFilePath)] =
    tasks.util.retryFuture(importFile1(f, path), 4)

  def importFile1(f: File, path: ProposedManagedFilePath)
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

  def createSource(path: ManagedFilePath): Source[ByteString, _] =
    s3stream.getData(S3Location(bucketName, assembleName(path)),
                     parallelism = 1)

  def exportFile(path: ManagedFilePath): Future[File] = {
    val file = TempFile.createTempFile("")
    val s3loc = S3Location(bucketName, assembleName(path))

    val f1 = s3stream
      .getData(s3loc, parallelism = 1)
      .runWith(FileIO.toPath(file.toPath))

    f1.flatMap(_ => s3stream.getMetadata(s3loc)).map { metadata =>
      val (size1, _) = getLengthAndHash(metadata)
      if (size1 !== file.length)
        throw new RuntimeException("S3: Downloaded file length != metadata")

      file
    }

  }

  def uri(mp: ManagedFilePath): tasks.util.Uri =
    tasks.util.Uri("s3://" + bucketName + "/" + assembleName(mp))

}
