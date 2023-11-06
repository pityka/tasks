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

import tasks.util.Uri
import cats.effect.IO
import org.http4s.client.Client
import org.http4s.Request
import org.http4s.Method
import org.http4s.headers.ETag
import org.http4s.RangeUnit
import org.http4s
import fs2.Chunk

class StreamHelper(
    s3Client: Option[tasks.fileservice.s3.S3],
    httpClient: Option[Client[IO]]
) {


  def s3Loc(uri: Uri) = {
    val bucket = uri.authority.toString
    val key = uri.path.toString.drop(1)
    (bucket, key)
  }

  private def createStreamHttp(
      uri: Uri,
      fromOffset: Long
  ): fs2.Stream[IO, Byte] = {
    assert(
      fromOffset == 0L,
      "Seeking into http not implemented yet. Use Range request header or drop the stream to implement it."
    )
    val partSize = 5242880L * 10L
    val retries = 3
    val parallelism = 4

    def bySingleRequest =
      fs2.Stream.force(
        httpClient.get
          .run(Request(method = Method.GET, uri = uri.http4s))
          .allocated
          .map { case (response, release) =>
            response.body.onFinalize(release)
          }
      )

    def getRangeOnce(range: http4s.headers.Range.SubRange) =
      fs2.Stream.force(
        httpClient.get
          .run(
            Request(
              method = Method.GET,
              uri = uri.http4s,
              headers = org.http4s.Headers(
                org.http4s.headers.Range(
                  unit = RangeUnit.Bytes,
                  ranges = cats.data.NonEmptyList(range, Nil)
                )
              )
            )
          )
          .allocated
          .map { case (response, release) =>
            response.body.onFinalize(release)
          }
      )

    def serverIndicatesAcceptRanges =
      httpClient.get
        .run(Request(method = Method.HEAD, uri = uri.http4s))
        .use { response =>
          val clength = response.contentLength
          val acceptsRanges = response.headers
            .get[org.http4s.headers.`Accept-Ranges`]
            .filter(_.rangeUnits == RangeUnit.Bytes)
            .isDefined
          IO.pure((clength, acceptsRanges))
        }

    def serverRespondsWithCorrectPartialContent(contentLength: Long) =
      if (contentLength < 10L) IO.pure(true)
      else {
        val max = contentLength - 1
        val min = contentLength - 10
        httpClient.get
          .run(
            Request(
              method = Method.GET,
              uri = uri.http4s,
              headers = org.http4s.Headers(
                org.http4s.headers.Range(
                  unit = RangeUnit.Bytes,
                  ranges = cats.data.NonEmptyList(
                    http4s.headers.Range.SubRange(min, Some(max)),
                    Nil
                  )
                )
              )
            )
          )
          .use { response =>
            IO.pure(response.status == org.http4s.Status.PartialContent)
          }
      }

    def getHeader = serverIndicatesAcceptRanges.flatMap {
      case d @ (Some(contentLength), true) =>
        serverRespondsWithCorrectPartialContent(contentLength).map {
          case true => d
          case false =>
            scribe.debug(
              s"$uri returned `Accept-Ranges` but did not respond with 206 on probing request."
            )
            (Some(contentLength), false)
        }
      case other => IO.pure(other)
    }

    def makeParts(contentLength: Long): IndexedSeq[IO[Chunk[Byte]]] = {

      val intervals = 0L until contentLength by partSize map (s =>
        (s, math.min(contentLength, s + partSize))
      )

      intervals
        .map { case (startIdx, openEndIdx) =>
          IO.unit *> {
            val rangeHeader =
              http4s.headers.Range.SubRange(startIdx, openEndIdx - 1)
            tasks.util.retryIO(s"$uri @($startIdx-${openEndIdx - 1})")(
              getRangeOnce(rangeHeader).compile
                .foldChunks(Chunk.empty[Byte])(_ ++ _)
                .map { data =>
                  val expectedLength = openEndIdx - startIdx
                  if (data.size != expectedLength)
                    throw new RuntimeException(
                      s"Expected download length does not match. Got ${data.size} for header $rangeHeader."
                    )

                  data
                },
              retries
            )(scribe.Logger[StreamHelper])
          }
        }
    }

    def byPartsWithRetry(contentLength: Long): fs2.Stream[IO, Chunk[Byte]] =
      fs2.Stream
        .apply[IO, IO[Chunk[Byte]]](makeParts(contentLength): _*)
        .mapAsync(parallelism) { fetchPart =>
          fetchPart
        }
    if (httpClient.isEmpty)
      throw new RuntimeException(
        "Can't make http request because it was not enabled."
      )

    fs2.Stream
      .eval(
        getHeader.map {
          case (Some(contentLength), true) =>
            scribe.debug(s"Fetching $uri by parts.")
            byPartsWithRetry(contentLength).unchunks
          case _ =>
            scribe.debug(s"Fetching $uri by one request.")
            bySingleRequest
        }
      )
      .flatMap(identity)

  }

  private def createStreamS3(
      uri: Uri,
      fromOffset: Long
  ): fs2.Stream[IO, Byte] = {
    assert(
      fromOffset == 0L,
      "Seeking into S3 file not implemented. Use GetObjectMetaData.range to implement it."
    )
    assert(s3Client.isDefined, "S3Client is not started (enable in config)")
    val (bucket, key) = s3Loc(uri)
    val length = s3Client.get
      .getObjectMetadata(bucket, key)
      .map { m =>
        if (m.isEmpty)
          throw new RuntimeException(s"S3: File does not exists $uri")
        m.get.contentLength
      }

    fs2.Stream.force(length.map { length =>
      if (length > 1024 * 1024 * 10)
        s3Client.get.readFileMultipart(bucket, key, 1)
      else s3Client.get.readFile(bucket, key)
    })
  }

  private def createStreamFile(
      uri: Uri,
      fromOffset: Long
  ): fs2.Stream[IO, Byte] = {
    val file = new java.io.File(uri.path)
    fs2.io.file
      .Files[IO]
      .readRange(
        fs2.io.file.Path.fromNioPath(file.toPath()),
        chunkSize = 8192,
        start = fromOffset,
        end = Long.MaxValue
      )
  }

  def createStream(uri: Uri, fromOffset: Long): fs2.Stream[IO, Byte] =
    uri.scheme match {
      case "http" | "https" => createStreamHttp(uri, fromOffset)
      case "s3"             => createStreamS3(uri, fromOffset)
      case "file"           => createStreamFile(uri, fromOffset)
    }

  private def getContentLengthAndETagHttp(
      uri: Uri
  ): IO[(Option[Long], Option[String])] = {
    assert(
      httpClient.isDefined,
      "http client is not started (enable in config)"
    )

    httpClient.get.run(Request(uri = uri.http4s, method = Method.HEAD)).use {
      response =>
        val etag = response.headers.get[ETag].map(_.tag.tag)
        val clength = response.contentLength
        response.body.compile.drain.map { _ =>
          clength -> etag
        }
    }
  }

  private def getContentLengthAndETagS3(
      uri: Uri
  ): IO[(Option[Long], Option[String])] = {
    val (bucket, key) = s3Loc(uri)
    assert(s3Client.isDefined, "S3Client is not started (enable in config)")

    s3Client.get
      .getObjectMetadata(bucket, key)
      .map {
        case None    => (None, None)
        case Some(x) => Some(x.contentLength.toLong) -> Some(x.eTag())
      }

  }

  def getContentLengthAndETag(
      uri: Uri
  ): IO[(Option[Long], Option[String])] = uri.scheme match {
    case "http" | "https" => getContentLengthAndETagHttp(uri)
    case "s3"             => getContentLengthAndETagS3(uri)
  }

}
