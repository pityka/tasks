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
import akka.stream.scaladsl._
import akka.http.scaladsl.model.{HttpRequest, headers, HttpMethods, StatusCodes}
import akka.util._
import scala.concurrent.{ExecutionContext, Future}
import tasks.util.Uri
import akka.http.scaladsl.Http
import akka.stream.alpakka.s3.scaladsl.S3
import akka.event.LoggingAdapter

class StreamHelper(implicit
    as: ActorSystem,
    actorMaterializer: Materializer,
    ec: ExecutionContext
) {

  implicit val log : LoggingAdapter = akka.event.Logging(as.eventStream, getClass)

  val queue = (rq: HttpRequest) => {
    log.debug("Queueing: " + rq)
    Http(as).singleRequest(rq)
  }

  def s3Loc(uri: Uri) = {
    val bucket = uri.authority.toString
    val key = uri.path.toString.drop(1)
    (bucket, key)
  }

  private def createSourceHttp(
      uri: Uri,
      fromOffset: Long
  ): Source[ByteString, _] = {
    assert(
      fromOffset == 0L,
      "Seeking into http not implemented yet. Use Range request header or drop the stream to implement it."
    )
    val partSize = 5242880L * 10L
    val retries = 3
    val parallelism = 4

    def bySingleRequest =
      Source
        .lazyFuture(() => queue(HttpRequest(uri = uri.akka)))
        .map(_.entity.dataBytes)
        .flatMapConcat(identity)

    def getRangeOnce(range: headers.ByteRange) =
      Source
        .lazyFuture(() =>
          queue(HttpRequest(uri = uri.akka).addHeader(headers.`Range`(range)))
        )
        .map(_.entity.dataBytes)
        .flatMapConcat(identity)

    def serverIndicatesAcceptRanges =
      queue(HttpRequest(uri = uri.akka).withMethod(HttpMethods.HEAD)).map {
        response =>
          response.discardEntityBytes();
          val clength = response.entity.contentLengthOption
          val acceptsRanges = response
            .header[headers.`Accept-Ranges`]
            .filter(_.value == "bytes")
            .isDefined
          (clength, acceptsRanges)
      }

    def serverRespondsWithCorrectPartialContent(contentLength: Long) =
      if (contentLength < 10L) Future.successful(true)
      else {
        val max = contentLength - 1
        val min = contentLength - 10
        val rq = HttpRequest(uri = uri.akka)
          .addHeader(headers.`Range`(headers.ByteRange(min, max)))
        queue(rq).map { response =>
          response.discardEntityBytes();
          log.debug(
            s"Probed content range capabilities of server. $rq responds with $response."
          )
          response.status == StatusCodes.PartialContent
        }
      }

    def getHeader = serverIndicatesAcceptRanges.flatMap {
      case d @ (Some(contentLength), true) =>
        serverRespondsWithCorrectPartialContent(contentLength).map {
          case true => d
          case false =>
            log.debug(
              s"$uri returned `Accept-Ranges` but did not respond with 206 on probing request."
            )
            (Some(contentLength), false)
        }
      case other => Future.successful(other)
    }

    def makeParts(contentLength: Long) = {

      val intervals = 0L until contentLength by partSize map (s =>
        (s, math.min(contentLength, s + partSize))
      )

      intervals
        .map { case (startIdx, openEndIdx) =>
          () => {
            val rangeHeader = headers.ByteRange(startIdx, openEndIdx - 1)
            tasks.util.retryFuture(s"$uri @($startIdx-${openEndIdx - 1})")(
              getRangeOnce(rangeHeader)
                .runFold(ByteString())(_ ++ _)
                .map { data =>
                  val expectedLength = openEndIdx - startIdx
                  if (data.size != expectedLength)
                    throw new RuntimeException(
                      s"Expected download length does not match. Got ${data.size} for header $rangeHeader."
                    )

                  data
                },
              retries
            )
          }
        }
    }

    def byPartsWithRetry(contentLength: Long) =
      Source
        .apply(makeParts(contentLength))
        .mapAsync(parallelism) { fetchPart =>
          fetchPart()
        }
        .async

    Source
      .lazyFuture(() =>
        getHeader.map {
          case (Some(contentLength), true) =>
            log.debug(s"Fetching $uri by parts.")
            byPartsWithRetry(contentLength)
          case _ =>
            log.debug(s"Fetching $uri by one request.")
            bySingleRequest
        }
      )
      .flatMapConcat(identity)

  }

  private def createSourceS3(
      uri: Uri,
      fromOffset: Long
  ): Source[ByteString, _] = {
    assert(
      fromOffset == 0L,
      "Seeking into S3 file not implemented. Use GetObjectMetaData.range to implement it."
    )
    val (bucket,key) = s3Loc(uri)
    S3.download(bucket,key).flatMapConcat{
      case Some((src,_)) => src 
      case _ => Source.empty
    }
  }

  private def createSourceFile(
      uri: Uri,
      fromOffset: Long
  ): Source[ByteString, _] = {
    val file = new java.io.File(uri.path)
    FileIO.fromPath(file.toPath, chunkSize = 8192, startPosition = fromOffset)
  }

  def createSource(uri: Uri, fromOffset: Long) = uri.scheme match {
    case "http" | "https" => createSourceHttp(uri, fromOffset)
    case "s3"             => createSourceS3(uri, fromOffset)
    case "file"           => createSourceFile(uri, fromOffset)
  }

  private def getContentLengthAndETagHttp(
      uri: Uri
  ): Future[(Option[Long], Option[String])] =
    queue(HttpRequest(uri = uri.akka).withMethod(HttpMethods.HEAD)).map(x => {
      x.discardEntityBytes();
      val etag = x.header[headers.`ETag`].map(_.value)
      val clength = x.entity.contentLengthOption
      (clength, etag)
    })

  private def getContentLengthAndETagS3(
      uri: Uri
  ): Future[(Option[Long], Option[String])] = {
    val (bucket,key) = s3Loc(uri)
    S3.getObjectMetadata(bucket,key).map{
      case None => (None,None)
      case Some(x) => Some(x.contentLength) -> x.eTag
    }.runWith(Sink.head)
  }
   

  def getContentLengthAndETag(
      uri: Uri
  ): Future[(Option[Long], Option[String])] = uri.scheme match {
    case "http" | "https" => getContentLengthAndETagHttp(uri)
    case "s3"             => getContentLengthAndETagS3(uri)
  }

}
