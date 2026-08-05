package tasks.fileservice

import java.io.File
import java.nio.file.{AtomicMoveNotSupportedException, Files, StandardCopyOption}

import cats.effect.IO
import cats.effect.kernel.Resource
import tasks.util.config.TasksConfig

private[tasks] object LocalFileCache {

  private val rootConfigPath = "tasks.fileservice.localFileCacheRoot"

  private def configuredRoot(config: TasksConfig): Option[File] =
    if (config.raw.hasPath(rootConfigPath)) {
      val value = config.raw.getString(rootConfigPath).trim
      if (value.isEmpty) None else Some(new File(value))
    } else None

  private def cacheKey(sf: SharedFile): String = sf.path match {
    case ManagedFilePath(elements) => "managed::" + elements.mkString("/")
    case RemoteFilePath(uri)       => "remote::" + uri.toString
  }

  private def sanitize(s: String): String =
    s.map(c => if (c.isLetterOrDigit || c == '.' || c == '-' || c == '_') c else '_')
      .take(80)

  private def digestHex(s: String): String =
    java.security.MessageDigest
      .getInstance("SHA-256")
      .digest(s.getBytes("UTF-8"))
      .map(b => f"$b%02x")
      .mkString

  private def targetFile(root: File, sf: SharedFile): File =
    new File(root, digestHex(cacheKey(sf)) + "-" + sanitize(sf.name))

  private def contentHashMatches(file: File, expectedHash: Int): IO[Boolean] =
    FileStorage
      .getContentHash(
        fs2.io.file.Files[IO].readAll(fs2.io.file.Path.fromNioPath(file.toPath))
      )
      .map(_ == expectedHash)
      .handleError(_ => false)

  private def isValid(file: File, sf: SharedFile, config: TasksConfig): IO[Boolean] =
    IO(file.isFile && file.length == sf.byteSize).flatMap {
      case false => IO.pure(false)
      case true =>
        if (sf.hash == -1 || config.skipContentHashVerificationAfterCache) IO.pure(true)
        else contentHashMatches(file, sf.hash)
    }

  private def persistInto(source: File, target: File): IO[File] = IO {
    val parent = target.getParentFile
    if (parent != null) Files.createDirectories(parent.toPath)
    val staging = new File(parent, target.getName + ".part-" + java.util.UUID.randomUUID())
    Files.copy(source.toPath, staging.toPath, StandardCopyOption.REPLACE_EXISTING)
    try
      Files.move(
        staging.toPath,
        target.toPath,
        StandardCopyOption.REPLACE_EXISTING,
        StandardCopyOption.ATOMIC_MOVE
      )
    catch {
      case _: AtomicMoveNotSupportedException =>
        Files.move(staging.toPath, target.toPath, StandardCopyOption.REPLACE_EXISTING)
    }
    target
  }

  def wrap(sf: SharedFile, keepLocalCache: Boolean, download: Resource[IO, File])(implicit
      config: TasksConfig
  ): Resource[IO, File] =
    if (!keepLocalCache) download
    else
      configuredRoot(config) match {
        case None => download
        case Some(root) =>
          val target = targetFile(root, sf)
          Resource.eval(isValid(target, sf, config)).flatMap {
            case true  => Resource.pure[IO, File](target)
            case false => Resource.eval(download.use(persistInto(_, target)))
          }
      }

}
