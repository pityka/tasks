package tasks.caching

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.effect.kernel.Resource
import fs2.{Stream, Pipe}
import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import org.ekrich.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import tasks.util.Uri
import tasks.util.config.TasksConfig
import tasks.fileservice._
import tasks.queue.{HashedTaskDescription, TaskId, UntypedResult, Base64Data}

class CacheErrorPropagationTest extends AnyFunSuite with Matchers {

  implicit val tconfig: TasksConfig =
    tasks.util.config.parse(() => ConfigFactory.load())

  implicit val streamHelper: StreamHelper = new StreamHelper(
    s3Client = None,
    httpClient = None,
    s3DownloadPartSizeMB = 1,
    s3DownloadParallelism = 64,
    s3MultipartThreshold = 1024L * 1024 * 10
  )

  implicit val prefix: FileServicePrefix = FileServicePrefix(Vector("test"))

  val htd: HashedTaskDescription = HashedTaskDescription(TaskId("t", 1), "hash")

  class StubManagedStorage(
      containsImpl: IO[Option[SharedFile]],
      calls: AtomicInteger
  ) extends ManagedFileStorage {
    def uri(mp: ManagedFilePath): IO[Uri] = IO.pure(Uri("file:///x"))
    def stream(path: ManagedFilePath, fromOffset: Long): Stream[IO, Byte] =
      Stream.raiseError[IO](new RuntimeException("not supported"))
    def contains(path: ManagedFilePath, size: Long, hash: Int): IO[Boolean] =
      IO.pure(false)
    def contains(
        path: ManagedFilePath,
        retrieveSizeAndHash: Boolean
    ): IO[Option[SharedFile]] =
      IO(calls.incrementAndGet()) *> containsImpl
    def sink(
        path: ProposedManagedFilePath
    ): Pipe[IO, Byte, (Long, Int, ManagedFilePath)] = { _ =>
      Stream.exec(IO(calls.incrementAndGet()).void) ++
        Stream.raiseError[IO](new RuntimeException("Simulated write failure"))
    }
    def exportFile(path: ManagedFilePath): Resource[IO, File] =
      Resource.raiseError[IO, File, Throwable](
        new RuntimeException("not supported")
      )
    def sharedFolder(folders: Seq[String]): IO[Option[File]] = IO.pure(None)
    def delete(
        path: ManagedFilePath,
        expectedSize: Long,
        expectedHash: Int
    ): IO[Boolean] = IO.pure(false)
  }

  def makeCache(storage: ManagedFileStorage): SharedFileCache = {
    implicit val fsc: FileServiceComponent =
      FileServiceComponent(storage, new RemoteFileStorage)
    new SharedFileCache
  }

  test("SharedFileCache.get returns None for a genuinely absent cache entry") {
    val calls = new AtomicInteger(0)
    val cache = makeCache(new StubManagedStorage(IO.pure(None), calls))
    cache.get(htd).unsafeRunSync() shouldBe None
    calls.get() shouldBe 1
  }

  test(
    "SharedFileCache.get retries then raises on a persistent read failure instead of swallowing it as a miss"
  ) {
    val calls = new AtomicInteger(0)
    val cache = makeCache(
      new StubManagedStorage(
        IO.raiseError(new RuntimeException("Simulated read failure")),
        calls
      )
    )
    assertThrows[RuntimeException] {
      cache.get(htd).unsafeRunSync()
    }
    calls.get() shouldBe 4
  }

  test(
    "SharedFileCache.set retries then raises on a persistent write failure instead of swallowing it"
  ) {
    val calls = new AtomicInteger(0)
    val cache = makeCache(new StubManagedStorage(IO.pure(None), calls))
    val result = UntypedResult(Set.empty, Base64Data(""), None)
    assertThrows[RuntimeException] {
      cache.set(htd, result).unsafeRunSync()
    }
    calls.get() shouldBe 4
  }
}
