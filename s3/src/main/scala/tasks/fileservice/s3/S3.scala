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
import cats.effect.std.Semaphore
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
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import S3Client.{S3UploadResponse, ObjectMetaData}

object S3 {
  def makeS3ClientResource(
      regionProfileName: Option[String],
      httpMaxConcurrency: Option[Int],
      httpMaxPendingConnectionAcquires: Option[Int],
      httpConnectionAcquisitionTimeout: Option[FiniteDuration],
      maxConcurrentRequests: Option[Int]
  ) = {
    val rateLimiter: Resource[IO, Option[Semaphore[IO]]] =
      maxConcurrentRequests match {
        case Some(n) if n > 0 =>
          Resource.eval(Semaphore[IO](n.toLong).map(Some(_)))
        case _ => Resource.pure(None)
      }
    rateLimiter.flatMap { sem =>
      Resource.make[IO, tasks.fileservice.s3.S3](IO {

        val s3AWSSDKClient =
          tasks.fileservice.s3.S3
            .makeAWSSDKClient(
              regionProfileName,
              httpMaxConcurrency,
              httpMaxPendingConnectionAcquires,
              httpConnectionAcquisitionTimeout
            )

        new tasks.fileservice.s3.S3(s3AWSSDKClient, sem)

      })(v => IO { v.s3.close })
    }
  }
  def makeAWSSDKClient(
      regionProfileName: Option[String],
      httpMaxConcurrency: Option[Int],
      httpMaxPendingConnectionAcquires: Option[Int],
      httpConnectionAcquisitionTimeout: Option[FiniteDuration]
  ) = {
    val builder = S3AsyncClient
      .builder()
      .credentialsProvider(DefaultCredentialsProvider.create())
      .region({
        val b = DefaultAwsRegionProviderChain.builder()
        regionProfileName
          .foldLeft(b)((a, b) => a.profileName(b))
          .build
          .getRegion()
      })
    if (
      httpMaxConcurrency.isDefined ||
      httpMaxPendingConnectionAcquires.isDefined ||
      httpConnectionAcquisitionTimeout.isDefined
    ) {
      val nettyBuilder = NettyNioAsyncHttpClient.builder()
      httpMaxConcurrency.foreach(nettyBuilder.maxConcurrency(_))
      httpMaxPendingConnectionAcquires.foreach(
        nettyBuilder.maxPendingConnectionAcquires(_)
      )
      httpConnectionAcquisitionTimeout.foreach(d =>
        nettyBuilder.connectionAcquisitionTimeout(Duration.ofNanos(d.toNanos))
      )
      builder.httpClient(nettyBuilder.build()).build()
    } else builder.build()
  }

  // S3 multipart limits:
  //   - parts numbered 1..10000
  //   - each part >= 5 MiB (except the last)
  //   - each part <= 5 GiB
  //   - object <= 5 TiB
  private[s3] val maxPartSizeBytes: Int = 1900000000
  private[s3] val partSizeDoublingInterval: Int = 800

  private[s3] def partSizeForIndex(
      partNum: Long,
      initialBytes: Int
  ): Int = {
    val tier = ((partNum - 1) / partSizeDoublingInterval).toInt
    val safeTier = math.max(0, math.min(tier, 40))
    val grown = initialBytes.toLong * (1L << safeTier)
    math.min(grown, maxPartSizeBytes.toLong).toInt
  }

  private[s3] def growingChunks(
      initialBytes: Int
  ): Pipe[IO, Byte, (Chunk[Byte], Long)] = { in =>
    def go(
        stream: fs2.Stream[IO, Byte],
        buffer: Chunk.Queue[Byte],
        partNum: Long
    ): Pull[IO, (Chunk[Byte], Long), Unit] = {
      val target = partSizeForIndex(partNum, initialBytes)
      if (buffer.size >= target) {
        val head = buffer.take(target)
        val tail = buffer.drop(target)
        Pull.output1((head, partNum)) >> go(stream, tail, partNum + 1)
      } else {
        stream.pull.uncons.flatMap {
          case Some((c, rest)) =>
            go(rest, buffer :+ c, partNum)
          case None =>
            if (buffer.size > 0) Pull.output1((buffer, partNum))
            else Pull.done
        }
      }
    }
    go(in, Chunk.Queue.empty[Byte], 1L).stream
  }
}

/** Wrapper of AWS SDK's S3AsyncClient into fs2.Stream and cats.effect.IO
  *
  * @param s3
  */
class S3(
    val s3: S3AsyncClient,
    rateLimiter: Option[Semaphore[IO]]
) extends tasks.fileservice.s3.S3Client {
  private type PartId = Long
  private type PartLength = Long
  private type UploadId = String

  def io[J, A](fut: => CompletableFuture[A]): IO[A] = {
    val call = IO.fromCompletableFuture(IO.delay(fut))
    rateLimiter.fold(call)(_.permit.use(_ => call))
  }

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

  /** Uploads a file in multiple parts. Suitable for big files.
    *
    * The part size starts at @partSize MB and doubles every
    * `S3.partSizeDoublingInterval` parts (capped at `S3.maxPartSizeBytes`).
    * This keeps the part count under S3's 10000-part limit for streams up to
    * the 5 TiB object-size ceiling, regardless of the initial @partSize.
    *
    * Memory use is proportional to @partSize * @multiPartConcurrency at the
    * start of the upload, and grows alongside the chunk size as more parts are
    * uploaded.
    *
    * Note: AWS S3 API does not support uploading empty files via multipart
    * upload. If no data passes through the stream, the multipart upload is
    * aborted and an empty object is uploaded via a single PutObject instead.
    *
    * For small files, consider using [[uploadFile]] instead.
    *
    * @param bucket
    *   the bucket name
    * @param key
    *   the target file key
    * @param partSize
    *   the initial part size indicated in MBs. It must be at least 5, as
    *   required by AWS.
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
    val initialChunkBytes = math.max(5, partSize) * 1048576

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
          in.through(S3.growingChunks(initialChunkBytes))
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
  def readFile(
      bucket: String,
      key: String,
      fromOffset: Long = 0L
  ): fs2.Stream[IO, Byte] =
    fs2.Stream
      .eval(
        io {
          val builder = GetObjectRequest
            .builder()
            .bucket(bucket)
            .key(key)
          if (fromOffset > 0L) builder.range(s"bytes=$fromOffset-")
          s3.getObject(
            builder.build(),
            AsyncResponseTransformer.toBytes[GetObjectResponse]
          )
        }
      )
      .flatMap(r =>
        fs2.Stream.chunk(Chunk(ArraySeq.unsafeWrapArray(r.asByteArray): _*))
      )

  /** Reads a file in multiple parts of the specified @partSize per request.
    * Suitable for big files.
    *
    * It does so in constant memory. So at a given time, only the number of
    * chunks indicated by @partSize * @multiPartConcurrency will be loaded in
    * memory.
    *
    * For small files, consider using [[readFile]] instead.
    *
    * @param partSize
    *   in megabytes
    * @param multiPartConcurrency
    *   the number of concurrent part downloads
    */
  def readFileMultipart(
      bucket: String,
      key: String,
      partSize: Int,
      multiPartConcurrency: Int,
      fromOffset: Long = 0L
  ): fs2.Stream[IO, Byte] = {
    val chunkSizeBytes = partSize.toLong * 1048576L

    def downloadPart(offset: Long): IO[Chunk[Byte]] =
      io(
        s3.getObject(
          GetObjectRequest
            .builder()
            .range(
              s"bytes=$offset-${offset + chunkSizeBytes - 1}"
            )
            .bucket(bucket)
            .key(key)
            .build(),
          AsyncResponseTransformer.toBytes[GetObjectResponse]
        )
      ).flatMap { resp =>
        IO.interruptible {
          Chunk.ByteBuffer(resp.asByteBuffer())
        }
      }

    fs2.Stream
      .eval(
        getObjectMetadata(bucket, key).flatMap {
          case None =>
            IO.raiseError(
              new RuntimeException(s"S3: File does not exist $bucket/$key")
            )
          case Some(meta) => IO.pure(meta.contentLength)
        }
      )
      .flatMap { totalSize =>
        val offsets =
          Iterator
            .iterate(fromOffset)(_ + chunkSizeBytes)
            .takeWhile(_ < totalSize)
            .toList

        fs2.Stream
          .emits(offsets)
          .covary[IO]
          .parEvalMap(multiPartConcurrency)(downloadPart)
          .unchunks
      }

  }
}
