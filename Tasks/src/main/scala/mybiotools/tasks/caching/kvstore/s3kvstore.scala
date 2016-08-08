/*
* The MIT License
*
* Copyright (c) 2016 Istvan Bartha
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

package tasks.caching.kvstore

import tasks.util._
import tasks.util.eq._
import tasks._

import java.io.ByteArrayInputStream

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.DeleteObjectRequest
import com.amazonaws.services.s3.model.StorageClass
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.ec2.model.SpotPlacement
import com.amazonaws.AmazonServiceException

sealed class S3LargeKVStore(bucketName: String, folderPrefix: String) extends LargeKeyKVStore(new S3KVStore(bucketName, folderPrefix))

sealed class S3KVStore(bucketName: String, folderPrefix: String) extends KVStoreWithDelete {

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

  private def assembleName(key: Array[Byte]) = (if (folderPrefix != "") folderPrefix + "/" else "") + javax.xml.bind.DatatypeConverter.printBase64Binary(key)

  def close: Unit = {
    tm.shutdownNow
  }

  def get(k: Array[Byte]): Option[Array[Byte]] = {
    val tr = retry(5) {
      try {
        Some(readStreamAndClose(
          s3Client
          .getObject(bucketName, assembleName(k))
          .getObjectContent
        )
          .toArray)
      } catch {
        case x: AmazonServiceException => None
      }
    }

    tr.failed.foreach(println)

    tr.getOrElse(None)
  }

  def listKeys: List[Array[Byte]] = ???

  def put(k: Array[Byte], v: Array[Byte]): Unit = retry(5) {

    val putrequest = new PutObjectRequest(bucketName, assembleName(k), new ByteArrayInputStream(v), new ObjectMetadata())

    { // set server side encryption
      val objectMetadata = new ObjectMetadata();
      objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
      putrequest.setMetadata(objectMetadata);
    }
    val upload = tm.upload(putrequest);
    upload.waitForCompletion

    val metadata = s3Client.getObjectMetadata(bucketName, assembleName(k))
    val size1 = metadata.getInstanceLength

    if (size1 !== v.length.toLong) throw new RuntimeException("S3: Uploaded file length != on disk")

    true

  }.get

  def delete(k: Array[Byte]): Unit =
    s3Client.deleteObject(new DeleteObjectRequest(bucketName, assembleName(k)))

}
