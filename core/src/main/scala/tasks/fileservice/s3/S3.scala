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

  case class ObjectMetaData(contentLength: Long, eTag: String)

  case class S3UploadResponse(etag: String, contentLength: Long)

}

trait S3Client {
  import S3Client._
  def getObjectMetadata(bucket: String, key: String): IO[Option[ObjectMetaData]]

  def readFileMultipart(
      bucket: String,
      key: String,
      partSize: Int
  ): fs2.Stream[IO, Byte]

  def readFile(bucket: String, key: String): fs2.Stream[IO, Byte]

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
