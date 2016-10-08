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

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.StorageClass
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.ec2.model.SpotPlacement
import com.amazonaws.AmazonServiceException
import java.net.URL

import collection.JavaConversions._
import java.io.{File, InputStream}

class S3Storage(bucketName: String, folderPrefix: String) extends FileStorage {

  def list(pattern: String): List[SharedFile] = ???

  def centralized = true

  @transient private var _client = new AmazonS3Client();

  @transient private var _tm = new TransferManager(s3Client);

  private def s3Client = {
    if (_client == null) {
      _client = new AmazonS3Client();
    }
    _client
  }

  private def tm = {
    if (_tm == null) {
      _tm = new TransferManager(s3Client);
    }
    _tm
  }

  def contains(path: ManagedFilePath, size: Long, hash: Int): Boolean = {
    val tr = retry(5) {
      try {
        val metadata =
          s3Client.getObjectMetadata(bucketName, assembleName(path))
        val (size1, hash1) = getLengthAndHash(metadata)
        size1 === size && (config.global.skipContentHashVerificationAfterCache || hash === hash1)
      } catch {
        case x: AmazonServiceException => false
      }
    }

    tr.failed.foreach(println)

    tr.getOrElse(false)
  }

  def getLengthAndHash(metadata: ObjectMetadata): (Long, Int) =
    metadata.getInstanceLength -> metadata.getETag.hashCode

  private def assembleName(path: ManagedFilePath) =
    ((if (folderPrefix != "") folderPrefix + "/" else "") +: path.pathElements)
      .filterNot(_ == "/")
      .map(x => if (x.startsWith("/")) x.drop(1) else x)
      .map(x => if (x.endsWith("/")) x.dropRight(1) else x)
      .mkString("/")

  def importFile(f: File, path: ProposedManagedFilePath)
    : Try[(Long, Int, File, ManagedFilePath)] = {
    val managed = path.toManaged

    retry(5) {

      val putrequest =
        new PutObjectRequest(bucketName, assembleName(managed), f)

      { // set server side encryption
        val objectMetadata = new ObjectMetadata();
        objectMetadata.setSSEAlgorithm(
            ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        putrequest.setMetadata(objectMetadata);
      }
      val upload = tm.upload(putrequest);
      upload.waitForCompletion

      val metadata =
        s3Client.getObjectMetadata(bucketName, assembleName(managed))
      val (size1, hash1) = getLengthAndHash(metadata)

      if (size1 !== f.length)
        throw new RuntimeException("S3: Uploaded file length != on disk")

      (size1, hash1, f, managed)

    }

  }

  def openStream(path: ManagedFilePath): Try[InputStream] = {
    val s3object = s3Client.getObject(bucketName, assembleName(path))
    scala.util.Success(s3object.getObjectContent)
  }

  def exportFile(path: ManagedFilePath): Try[File] = {
    val file = TempFile.createTempFile("")
    retry(5) {
      val download = tm.download(bucketName, assembleName(path), file)
      download.waitForCompletion

      val metadata = s3Client.getObjectMetadata(bucketName, assembleName(path))
      val (size1, hash1) = getLengthAndHash(metadata)
      if (size1 !== file.length)
        throw new RuntimeException("S3: Downloaded file length != metadata")

      file
    }
  }

  def url(mp: ManagedFilePath): URL =
    new URL("s3", bucketName, assembleName(mp))

}
