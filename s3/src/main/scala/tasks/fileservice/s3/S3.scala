// MIT License

// Copyright (c) 2018 Daniel Mateus Pires

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
package tasks.fileservice.s3

import cats.effect._
import fs2.{Chunk, Pipe, Pull}
import software.amazon.awssdk.core.async.{
  AsyncRequestBody,
  AsyncResponseTransformer
}
import software.amazon.awssdk.services.s3.model._

import java.nio.ByteBuffer
import scala.collection.immutable.ArraySeq
import scala.collection.immutable.ArraySeq.unsafeWrapArray
import software.amazon.awssdk.services.s3.S3AsyncClient
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import S3Client.{S3UploadResponse, ObjectMetaData}

object S3 {
  def makeS3ClientResource(regionProfileName: Option[String]) =
    Resource.make[IO, tasks.fileservice.s3.S3](IO {

      val s3AWSSDKClient =
        tasks.fileservice.s3.S3
          .makeAWSSDKClient(regionProfileName)

      new tasks.fileservice.s3.S3(s3AWSSDKClient)

    })(v => IO { v.s3.close })
  def makeAWSSDKClient(regionProfileName: Option[String]) = S3AsyncClient
    .builder()
    .credentialsProvider(DefaultCredentialsProvider.create())
    .region({
      val b = DefaultAwsRegionProviderChain.builder()
      regionProfileName
        .foldLeft(b)((a, b) => a.profileName(b))
        .build
        .getRegion()
    })
    .build()
}

/** Wrapper of AWS SDK's S3AsyncClient into fs2.Stream and cats.effect.IO
  *
  * @param s3
  */
class S3(val s3: S3AsyncClient) extends tasks.fileservice.s3.S3Client {
  private type PartId = Long
  private type PartLength = Long
  private type UploadId = String

  def io[J, A](fut: => CompletableFuture[A]): IO[A] =
    IO.fromCompletableFuture(IO.delay(fut))

  /** Deletes a file in a single request.
    */
  def delete(bucket: String, key: String): IO[Unit] =
    io(
      s3.deleteObject(
        DeleteObjectRequest
          .builder()
          .bucket(bucket)
          .key(key)
          .build()
      )
    ).void

  /** Uploads a file in a single request. Suitable for small files.
    *
    * For big files, consider using [[uploadFileMultipart]] instead.
    */
  def uploadFile(
      bucket: String,
      key: String,
      cannedAcl: List[String],
      serverSideEncryption: Option[String],
      grantFullControl: List[String]
  ): Pipe[IO, Byte, PutObjectResponse] =
    in =>
      fs2.Stream.eval {
        in.compile.toVector.flatMap { vs =>
          val bs = ByteBuffer.wrap(vs.toArray)
          val base = PutObjectRequest
            .builder()
            .bucket(bucket)
            .key(key)

          val builder = grantFullControl.foldLeft(
            serverSideEncryption.foldLeft(
              cannedAcl.foldLeft(base)((a, b) => a.acl(b))
            )((a, b) => a.serverSideEncryption(b))
          )((a, b) => a.grantFullControl(b))

          io(
            s3.putObject(
              builder
                .build(),
              AsyncRequestBody.fromByteBuffer(bs)
            )
          )
        }
      }

  /** Uploads a file in multiple parts of the specified @partSize per request.
    * Suitable for big files.
    *
    * It does so in constant memory. So at a given time, only the number of
    * bytes indicated by @partSize will be loaded in memory.
    *
    * Note: AWS S3 API does not support uploading empty files via multipart
    * upload. It does not gracefully respond on attempting to do this and
    * returns a `400` response with a generic error message. This function
    * accepts a boolean `uploadEmptyFile` (set to `false` by default) to
    * determine how to handle this scenario. If set to false (default) and no
    * data has passed through the stream, it will gracefully abort the
    * multi-part upload request. If set to true, and no data has passed through
    * the stream, an empty file will be uploaded on completion. An
    * `Option[ETag]` of `None` will be emitted on the stream if no file was
    * uploaded, else a `Some(ETag)` will be emitted. Alternatively, If you need
    * to create empty files, consider using consider using [[uploadFile]]
    * instead.
    *
    * For small files, consider using [[uploadFile]] instead.
    *
    * @param bucket
    *   the bucket name
    * @param key
    *   the target file key
    * @param partSize
    *   the part size indicated in MBs. It must be at least 5, as required by
    *   AWS.
    * @param uploadEmptyFiles
    *   whether to upload empty files or not, if no data has passed through the
    *   stream create an empty file default is false
    * @param multiPartConcurrency
    *   the number of concurrent parts to upload
    */
  def uploadFileMultipart(
      bucket: String,
      key: String,
      partSize: Int,
      multiPartConcurrency: Int,
      cannedAcl: List[String],
      serverSideEncryption: Option[String],
      grantFullControl: List[String]
  ): Pipe[IO, Byte, S3UploadResponse] = {
    val chunkSizeBytes = math.max(5, partSize) * 1048576

    def initiateMultipartUpload = {
      val base = CreateMultipartUploadRequest
        .builder()
        .bucket(bucket)
        .key(key)
      val builder = grantFullControl.foldLeft(
        serverSideEncryption.foldLeft(
          cannedAcl.foldLeft(base)((a, b) => a.acl(b))
        )((a, b) => a.serverSideEncryption(b))
      )((a, b) => a.grantFullControl(b))

      io(
        s3.createMultipartUpload(
          builder
            .build()
        )
      ).map(_.uploadId())
    }

    def uploadPart(
        uploadId: UploadId
    ): Pipe[
      IO,
      (Chunk[Byte], PartId),
      (UploadPartResponse, PartId, PartLength)
    ] =
      _.parEvalMap(multiPartConcurrency) { case (c, i) =>
        io(
          s3.uploadPart(
            UploadPartRequest
              .builder()
              .bucket(bucket)
              .key(key)
              .uploadId(uploadId)
              .partNumber(i.toInt)
              .contentLength(c.size.toLong)
              .build(),
            AsyncRequestBody.fromBytes(c.toArray)
          )
        ).map(r => (r, i, c.size.toLong))
      }

    def uploadEmptyFile = io(
      s3.putObject(
        PutObjectRequest.builder().bucket(bucket).key(key).build(),
        AsyncRequestBody.fromBytes(new Array[Byte](0))
      )
    )
    def completeUpload(
        uploadId: UploadId
    ): Pipe[IO, List[
      (UploadPartResponse, PartId, PartLength)
    ], S3UploadResponse] =
      _.evalMap {
        case Nil =>
          cancelUpload(uploadId).flatMap { _ =>
            uploadEmptyFile.map(r =>
              S3UploadResponse(etag = r.eTag(), contentLength = 0L)
            )

          }
        case tags =>
          val parts = tags.map { case (t, i, _) =>
            CompletedPart.builder().partNumber(i.toInt).eTag(t.eTag()).build()
          }.asJava
          io(
            s3.completeMultipartUpload(
              CompleteMultipartUploadRequest
                .builder()
                .bucket(bucket)
                .key(key)
                .uploadId(uploadId)
                .multipartUpload(
                  CompletedMultipartUpload.builder().parts(parts).build()
                )
                .build()
            )
          ).map { response =>
            S3UploadResponse(response.eTag(), tags.map(_._3).sum)
          }

      }

    def cancelUpload(uploadId: UploadId): IO[Unit] =
      io(
        s3.abortMultipartUpload(
          AbortMultipartUploadRequest
            .builder()
            .bucket(bucket)
            .key(key)
            .uploadId(uploadId)
            .build()
        )
      ).void

    in =>
      fs2.Stream
        .eval(initiateMultipartUpload)
        .flatMap { uploadId =>
          in.chunkN(chunkSizeBytes)
            .zip(fs2.Stream.iterate(1L)(_ + 1))
            .through(uploadPart(uploadId))
            .fold[List[(UploadPartResponse, PartId, PartLength)]](List.empty)(
              _ :+ _
            )
            .through(completeUpload(uploadId))
            .handleErrorWith(ex =>
              fs2.Stream.eval(cancelUpload(uploadId) >> Sync[IO].raiseError(ex))
            )
        }

  }

  def getObjectMetadata(
      bucket: String,
      key: String
  ): IO[Option[ObjectMetaData]] =
    io(
      s3.headObject(
        HeadObjectRequest
          .builder()
          .bucket(bucket)
          .key(key)
          .build()
      )
    ).map(response => ObjectMetaData(response.contentLength(), response.eTag()))
      .map(Option(_))
      .handleErrorWith(_ match {
        case _: NoSuchKeyException => IO.pure(Option.empty[ObjectMetaData])
        case e: Throwable          => IO.raiseError(e)
      })

  /** Reads a file in a single request. Suitable for small files.
    *
    * For big files, consider using [[readFileMultipart]] instead.
    */
  def readFile(bucket: String, key: String): fs2.Stream[IO, Byte] =
    fs2.Stream
      .eval(
        io(
          s3.getObject(
            GetObjectRequest
              .builder()
              .bucket(bucket)
              .key(key)
              .build(),
            AsyncResponseTransformer.toBytes[GetObjectResponse]
          )
        )
      )
      .flatMap(r =>
        fs2.Stream.chunk(Chunk(ArraySeq.unsafeWrapArray(r.asByteArray): _*))
      )

  /** Reads a file in multiple parts of the specified @partSize per request.
    * Suitable for big files.
    *
    * It does so in constant memory. So at a given time, only the number of
    * bytes indicated by @partSize will be loaded in memory.
    *
    * For small files, consider using [[readFile]] instead.
    *
    * @param partSize
    *   in megabytes
    */
  def readFileMultipart(
      bucket: String,
      key: String,
      partSize: Int
  ): fs2.Stream[IO, Byte] = {
    val chunkSizeBytes = partSize * 1048576

    // Range must be in the form "bytes=0-500" -> https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
    def go(offset: Long): Pull[IO, Byte, Unit] =
      fs2.Stream
        .eval {
          io(
            s3.getObject(
              GetObjectRequest
                .builder()
                .range(s"bytes=$offset-${offset + chunkSizeBytes}")
                .bucket(bucket)
                .key(key)
                .build(),
              AsyncResponseTransformer.toBytes[GetObjectResponse]
            )
          )
        }
        .pull
        .last
        .flatMap {
          case Some(resp) =>
            Pull.eval {
              IO.interruptible {
                val bs = resp.asByteArray()
                val len = bs.length
                if (len < 0) None else Some(Chunk(unsafeWrapArray(bs): _*))
              }
            }
          case None =>
            Pull.eval(IO.pure(None))
        }
        .flatMap {
          case Some(o) =>
            if (o.size < chunkSizeBytes) Pull.output(o)
            else Pull.output(o) >> go(offset + o.size)
          case None => Pull.done
        }

    go(0).stream
  }

}
