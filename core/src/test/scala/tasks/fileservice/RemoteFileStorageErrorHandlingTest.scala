package tasks.fileservice

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.ekrich.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import tasks.util.config.TasksConfig
import tasks.util.Uri

class FailingStreamHelper(implicit config: TasksConfig)
    extends StreamHelper(
      s3Client = None,
      httpClient = None,
      s3DownloadPartSizeMB = 1,
      s3DownloadParallelism = 64,
      s3MultipartThreshold = 1024L * 1024 * 10
    ) {
  override def getContentLengthAndETag(
      uri: Uri
  ): IO[(Option[Long], Option[String])] =
    IO.raiseError(new RuntimeException("Simulated network failure"))
}

class RemoteFileStorageErrorHandlingTest extends AnyFunSuite with Matchers {

  implicit val tconfig: TasksConfig =
    tasks.util.config.parse(() => ConfigFactory.load())

  test(
    "RemoteFileStorage.contains must propagate errors instead of swallowing them"
  ) {
    implicit val failingHelper: StreamHelper = new FailingStreamHelper
    val storage = new RemoteFileStorage

    val remotePath = RemoteFilePath(Uri("https://example.com/file.txt"))

    val result = storage.contains(remotePath, size = 1024, hash = 42)

    assertThrows[RuntimeException] {
      result.unsafeRunSync()
    }
  }

  test("RemoteFileStorage.contains retries on transient failures") {
    var attempts = 0

    val failThenSucceedHelper = new StreamHelper(
      s3Client = None,
      httpClient = None,
      s3DownloadPartSizeMB = 1,
      s3DownloadParallelism = 64,
      s3MultipartThreshold = 1024L * 1024 * 10
    ) {
      override def getContentLengthAndETag(
          uri: Uri
      ): IO[(Option[Long], Option[String])] =
        IO {
          attempts += 1
          if (attempts < 3) throw new RuntimeException("Transient failure")
          (Some(1024L), Some("abc123"))
        }
    }

    implicit val helper: StreamHelper = failThenSucceedHelper
    val storage = new RemoteFileStorage

    val remotePath = RemoteFilePath(Uri("https://example.nonexistent/file.txt"))
    val result =
      storage.contains(remotePath, size = 1024, hash = "abc123".hashCode)

    result.unsafeRunSync() should be(true)
    attempts should be >= 3
  }

}
