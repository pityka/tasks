/*
 * The MIT License
 *
 * Copyright (c) 2017 Istvan Bartha
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
import akka.stream._
import akka.stream.actor._
import akka.stream.scaladsl._
import akka.http.scaladsl.model._
import akka.http.scaladsl.Http
import akka.util._
import com.bluelabs.s3stream._
import scala.concurrent.{ExecutionContext, Future, Promise}

class StreamHelper(s3stream: Option[S3Stream])(
    implicit as: ActorSystem,
    actorMaterializer: ActorMaterializer,
    ec: ExecutionContext) {

  val queue = (rq: HttpRequest) => httpqueue.HttpQueue(as).queue(rq)

  def s3Loc(uri: Uri) = {
    val bucket = uri.authority.host.toString
    val key = uri.path.toString.drop(1)
    S3Location(bucket, key)
  }

  def createSourceHttp(uri: Uri): Source[ByteString, _] =
    Source
      .fromFuture(queue(HttpRequest(uri = uri)))
      .map(_.entity.dataBytes)
      .flatMapConcat(identity)

  def createSourceS3(uri: Uri): Source[ByteString, _] =
    s3stream.get.getData(s3Loc(uri))

  def createSource(uri: Uri) = uri.scheme match {
    case "http" | "https" => createSourceHttp(uri)
    case "s3" => createSourceS3(uri)
  }

  def getContentLengthAndETagHttp(
      uri: Uri): Future[(Option[Long], Option[String])] =
    queue(HttpRequest(uri = uri)).map(x => {
      x.discardEntityBytes();
      val etag = x.header[headers.`ETag`].map(_.value)
      val clength = x.header[headers.`Content-Length`].map(_.length)
      (clength, etag)
    })

  def getContentLengthAndETagS3(
      uri: Uri): Future[(Option[Long], Option[String])] =
    s3stream.get.getMetadata(s3Loc(uri)).map(x => x.contentLength -> x.eTag)

  def getContentLengthAndETag(
      uri: Uri): Future[(Option[Long], Option[String])] = uri.scheme match {
    case "http" | "https" => getContentLengthAndETagHttp(uri)
    case "s3" => getContentLengthAndETagS3(uri)
  }

}
