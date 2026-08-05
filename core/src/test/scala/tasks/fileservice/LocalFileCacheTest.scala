package tasks.fileservice

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import org.ekrich.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import tasks.util.TempFile
import tasks.util.config.TasksConfig

class CountingManagedFileStorage(data: Array[Byte]) extends ManagedFileStorage {

  val downloadCount = new AtomicInteger(0)

  def uri(mp: ManagedFilePath): IO[tasks.util.Uri] = ???
  def stream(path: ManagedFilePath, fromOffset: Long): fs2.Stream[IO, Byte] = ???
  def contains(path: ManagedFilePath, size: Long, hash: Int): IO[Boolean] = ???
  def contains(
      path: ManagedFilePath,
      retrieveSizeAndHash: Boolean
  ): IO[Option[SharedFile]] = ???
  def sink(
      path: ProposedManagedFilePath
  ): fs2.Pipe[IO, Byte, (Long, Int, ManagedFilePath)] = ???
  def sharedFolder(prefix: Seq[String]): IO[Option[File]] = IO.pure(None)
  def delete(path: ManagedFilePath, expectedSize: Long, expectedHash: Int): IO[Boolean] =
    IO.pure(false)

  def exportFile(path: ManagedFilePath): Resource[IO, File] =
    Resource.make(IO {
      downloadCount.incrementAndGet()
      val f = TempFile.createTempFile(".download")
      tasks.util.writeBinaryToFile(f.getAbsolutePath, data)
      f
    })(f => IO(f.delete()).map(_ => ()))

}

class LocalFileCacheTest extends AnyFunSuite with Matchers {

  implicit val streamHelper: StreamHelper =
    new StreamHelper(
      s3Client = None,
      httpClient = None,
      s3DownloadPartSizeMB = 1,
      s3DownloadParallelism = 64,
      s3MultipartThreshold = 1024L * 1024 * 10
    )

  def readBinaryFile(path: String): Array[Byte] =
    java.nio.file.Files.readAllBytes(new File(path).toPath)

  def configWithRoot(root: Option[File]): TasksConfig =
    tasks.util.config.parse(() =>
      root
        .map(r =>
          ConfigFactory.parseString(
            s"""tasks.fileservice.localFileCacheRoot = "${r.getAbsolutePath}"\n"""
          )
        )
        .getOrElse(ConfigFactory.parseString(""))
        .withFallback(ConfigFactory.load())
    )

  def sharedFileFor(data: Array[Byte]): SharedFile = {
    val hash =
      FileStorage.getContentHash(fs2.Stream.chunk(fs2.Chunk.array(data))).unsafeRunSync()
    SharedFileHelper.create(data.length.toLong, hash, ManagedFilePath(Vector("proba")))
  }

  def freshCacheRoot(): File = {
    val f = TempFile.createTempFile("localfilecacheroot")
    f.delete()
    f
  }

  test("filePersistent avoids re-download across separate sessions and keeps the file") {
    val data = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8)
    val sf = sharedFileFor(data)
    val cacheRoot = freshCacheRoot()
    implicit val tconfig: TasksConfig = configWithRoot(Some(cacheRoot))

    val backend = new CountingManagedFileStorage(data)
    implicit val service: FileServiceComponent =
      FileServiceComponent(backend, new RemoteFileStorage)

    val firstPath = SharedFileHelper
      .getPathToFileNonCachedFile(sf, keepLocalCache = true)
      .use(f => IO(f.getCanonicalFile))
      .unsafeRunSync()

    backend.downloadCount.get() should equal(1)
    readBinaryFile(firstPath.getCanonicalPath).toVector should equal(data.toVector)
    firstPath.exists() should be(true)

    val secondContent = SharedFileHelper
      .getPathToFileNonCachedFile(sf, keepLocalCache = true)
      .use(f => IO(readBinaryFile(f.getCanonicalPath).toVector))
      .unsafeRunSync()

    backend.downloadCount.get() should equal(1)
    secondContent should equal(data.toVector)
    firstPath.exists() should be(true)
  }

  test("default access still deletes the exported file on release") {
    val data = Array[Byte](9, 9, 9)
    val sf = sharedFileFor(data)
    implicit val tconfig: TasksConfig = configWithRoot(None)

    val backend = new CountingManagedFileStorage(data)
    implicit val service: FileServiceComponent =
      FileServiceComponent(backend, new RemoteFileStorage)

    val usedPath = SharedFileHelper
      .getPathToFileNonCachedFile(sf)
      .use(f => IO(f.getCanonicalFile))
      .unsafeRunSync()

    usedPath.exists() should be(false)
    backend.downloadCount.get() should equal(1)
  }

  test("a corrupted cache entry triggers a fresh download") {
    val data = Array[Byte](4, 2, 4, 2, 4, 2)
    val sf = sharedFileFor(data)
    val cacheRoot = freshCacheRoot()
    implicit val tconfig: TasksConfig = configWithRoot(Some(cacheRoot))

    val backend = new CountingManagedFileStorage(data)
    implicit val service: FileServiceComponent =
      FileServiceComponent(backend, new RemoteFileStorage)

    val firstPath = SharedFileHelper
      .getPathToFileNonCachedFile(sf, keepLocalCache = true)
      .use(f => IO(f.getCanonicalFile))
      .unsafeRunSync()

    backend.downloadCount.get() should equal(1)

    tasks.util.writeBinaryToFile(firstPath.getAbsolutePath, Array[Byte](0, 0, 0, 0, 0, 0))

    val repairedContent = SharedFileHelper
      .getPathToFileNonCachedFile(sf, keepLocalCache = true)
      .use(f => IO(readBinaryFile(f.getCanonicalPath).toVector))
      .unsafeRunSync()

    backend.downloadCount.get() should equal(2)
    repairedContent should equal(data.toVector)
  }

}
