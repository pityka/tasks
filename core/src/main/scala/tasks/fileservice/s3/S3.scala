package tasks.fileservice.s3

import tasks.util.Uri
import cats.effect.IO
import org.http4s.client.Client
import org.http4s.Request
import org.http4s.Method
import org.http4s.headers.ETag
import org.http4s.RangeUnit
import org.http4s
import fs2.Chunk
import fs2.Pipe

object S3Client {

  sealed trait ObjectMetaData {
    def contentLength: Long
    def eTag: String
  }
  object ObjectMetaData {
    private[tasks] final case class ObjectMetaDataImpl(
        contentLength: Long,
        eTag: String
    ) extends ObjectMetaData
    def apply(contentLength: Long, eTag: String): ObjectMetaData =
      ObjectMetaDataImpl(contentLength, eTag)
    def unapply(o: ObjectMetaData): Some[(Long, String)] =
      Some((o.contentLength, o.eTag))
  }

  sealed trait S3UploadResponse {
    def etag: String
    def contentLength: Long
  }
  object S3UploadResponse {
    private[tasks] final case class S3UploadResponseImpl(
        etag: String,
        contentLength: Long
    ) extends S3UploadResponse
    def apply(etag: String, contentLength: Long): S3UploadResponse =
      S3UploadResponseImpl(etag, contentLength)
    def unapply(r: S3UploadResponse): Some[(String, Long)] =
      Some((r.etag, r.contentLength))
  }

}

trait S3Client {
  import S3Client._
  def getObjectMetadata(bucket: String, key: String): IO[Option[ObjectMetaData]]

  def readFileMultipart(
      bucket: String,
      key: String,
      partSize: Int,
      multiPartConcurrency: Int,
      fromOffset: Long
  ): fs2.Stream[IO, Byte]

  def readFileMultipart(
      bucket: String,
      key: String,
      partSize: Int,
      multiPartConcurrency: Int
  ): fs2.Stream[IO, Byte] =
    readFileMultipart(bucket, key, partSize, multiPartConcurrency, 0L)

  def readFile(
      bucket: String,
      key: String,
      fromOffset: Long
  ): fs2.Stream[IO, Byte]

  def readFile(
      bucket: String,
      key: String
  ): fs2.Stream[IO, Byte] =
    readFile(bucket, key, 0L)

  def buffer(
      bucket: String,
      key: String
  ): IO[Option[Array[Byte]]]

  def uploadFileMultipart(
      bucket: String,
      key: String,
      partSize: Int,
      multiPartConcurrency: Int,
      cannedAcl: List[String],
      serverSideEncryption: Option[String],
      grantFullControl: List[String]
  ): Pipe[IO, Byte, S3UploadResponse]
}
