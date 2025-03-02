/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Modified work, Copyright (c) 2016 Istvan Bartha

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

import java.io.File
import tasks.util._
import tasks.util.eq._
import tasks.util.config._
import cats.effect.kernel.Resource
import cats.effect.IO
import fs2.Stream
import fs2.Pipe
import java.nio.file.StandardCopyOption

private[tasks] object FolderFileStorage {

  private[tasks] def getContentHashOfFile(file: File, skip: Boolean): IO[Int] =
    if (skip) IO.pure(0)
    else {
      FileStorage
        .getContentHash(
          fs2.io.file
            .Files[IO]
            .readAll(fs2.io.file.Path.fromNioPath(file.toPath))
        )
    }

}

private[tasks] class FolderFileStorage(val basePath: File)(implicit
    config: TasksConfig
) extends ManagedFileStorage {

  if (basePath.exists && !basePath.isDirectory)
    throw new IllegalArgumentException(s"$basePath exists and not a folder")
  else if (!basePath.exists) basePath.mkdirs

  if (!basePath.isDirectory)
    throw new RuntimeException(s"Could not create $basePath")

  override def toString =
    s"FolderFileStorage(basePath=$basePath)"

  private val canonicalBasePath = basePath.getCanonicalPath

  private val tempFolder = new File(basePath, "___TMP___")
  tempFolder.mkdirs()
  private val tmpFolderCanonicalPath = tempFolder.getCanonicalPath()

  private def createLocalTempFile() = {
    def try1(i: Int): File = if (i === 0)
      throw new RuntimeException(s"Could not create temp file in $tempFolder")
    else {
      val name = scala.util.Random.alphanumeric.take(128).mkString
      val f = new File(tempFolder, name)
      if (f.exists()) try1(i - 1) else f
    }

    try1(5)

  }

  def delete(
      mp: ManagedFilePath,
      expectedSize: Long,
      expectedHash: Int
  ): IO[Boolean] =
    if (config.allowDeletion) {

      {
        val file = assemblePath(mp)
        val sizeOnDiskNow = IO(file.length)
        val hash = FolderFileStorage.getContentHashOfFile(
          file,
          config.skipContentHashCreationUponImport
        )
        val canRead = IO(file.canRead)

        val sizeMatch = sizeOnDiskNow.map(_ === expectedSize)
        val contentMatch = hash.map(_ === expectedHash)

        //  canRead && (expectedSize < 0 || (sizeMatch && contentMatch))
        val canDelete = canRead.flatMap { canRead =>
          if (!canRead) IO.pure(false)
          else {
            if (expectedSize < 0) IO.pure(true)
            else
              sizeMatch.flatMap { sizeMatch =>
                if (sizeMatch) contentMatch
                else IO.pure(false)
              }

          }
        }
        canDelete.map { canDelete =>
          if (canDelete) {
            val deleted = file.delete
            scribe.warn(s"File deleted $file $mp : $deleted")
            deleted
          } else {
            scribe.warn(
              s"Not deleting file because its size or hash is different than expectation. $file $mp $sizeMatch $contentMatch"
            )
            false
          }
        }
      }
    } else {
      scribe.warn(s"File deletion disabled. Would have deleted $mp")
      IO.pure(false)
    }

  def sharedFolder(prefix: Seq[String]): IO[Option[File]] = IO {
    val folder = new File(
      basePath.getAbsolutePath + File.separator + prefix
        .mkString(File.separator)
    )
    folder.mkdirs
    Some(folder)
  }

  def contains(path: ManagedFilePath, size: Long, hash: Int): IO[Boolean] = {
    val f = assemblePath(path)
    IO.interruptible(f.canRead).memoize.flatMap { canRead =>
      val sizeOnDiskNow = IO.interruptible(f.length)
      val sizeMatch = sizeOnDiskNow
        .map(sizeOnDiskNow => (sizeOnDiskNow, sizeOnDiskNow === size))
      val hashMatch = FolderFileStorage
        .getContentHashOfFile(
          f,
          config.skipContentHashCreationUponImport
        )
        .map(_ === hash)
      val contentMatch =
        if (config.skipContentHashVerificationAfterCache) IO.pure(true)
        else
          canRead.flatMap { canRead =>
            if (!canRead) IO.pure(false)
            else hashMatch
          }

      val pass = canRead.flatMap { canRead =>
        if (!canRead) IO.pure(false)
        else {
          if (size < 0) IO.pure(true)
          else
            sizeMatch.flatMap { case (_, sizeMatch) =>
              if (!sizeMatch) IO.pure(false)
              else contentMatch
            }
        }
      }

      pass.flatMap { pass =>
        if (!pass) {
          for {
            canRead <- canRead
            sizeMatch <- sizeMatch
            contentMatch <- contentMatch
          } yield {
            scribe.debug(
              s"$this does not contain $path due to: canRead:$canRead, sizeMatch:${sizeMatch._2}   (sizeOnDisk:${sizeMatch._1} vs expected:$size), contentMatch:$contentMatch. FileOnDisk: $f "
            )
            pass
          }
        } else IO.pure(pass)
      }
      pass
    }
  }

  def contains(
      path: ManagedFilePath,
      retrieveSizeAndHash: Boolean
  ): IO[Option[SharedFile]] = {
    val f = assemblePath(path)
    IO.interruptible(f.canRead).flatMap { canRead =>
      val pass = canRead

      if (!pass) {
        scribe.debug(s"$this does not contain $path due to: canRead:$canRead")
        IO.pure(None)
      } else {
        if (retrieveSizeAndHash) {
          val size = f.length
          val hash = FolderFileStorage.getContentHashOfFile(
            f,
            config.skipContentHashCreationUponImport
          )
          hash.map { hash =>
            Some(SharedFileHelper.create(size, hash, path))
          }
        } else
          IO.pure(Some(SharedFileHelper.create(size = -1L, hash = 0, path)))
      }

    }
  }

  def stream(
      path: ManagedFilePath,
      fromOffset: Long
  ): Stream[IO, Byte] =
    Stream.unit.flatMap { _ =>
      fs2.io.file
        .Files[IO]
        .readRange(
          path = fs2.io.file.Path.fromNioPath(assemblePath(path).toPath),
          start = fromOffset,
          end = Long.MaxValue,
          chunkSize = 8192 * 8
        )
    }

  def exportFile(path: ManagedFilePath): Resource[IO, File] = {
    val file = assemblePath(path)
    if (file.canRead) Resource.pure(file)
    else {
      Resource.raiseError[IO, File, Throwable](
        new java.nio.file.NoSuchFileException(
          file.getAbsolutePath + " " + System.nanoTime
        )
      )
    }
  }
  private def copyFile(
      source: File,
      destination: File,
      canMove: Boolean
  ): Unit = {
    val parentFolder = destination.getParentFile
    parentFolder.mkdirs
    if (canMove) {
      val success = source.renameTo(destination)
      if (!success) copyFile(source, destination, false)

    } else {
      val tmp = new File(parentFolder, destination.getName + ".tmp")

      try {
        java.nio.file.Files.copy(
          source.toPath(),
          tmp.toPath(),
          java.nio.file.StandardCopyOption.REPLACE_EXISTING
        )
      } catch {
        case e: java.io.IOException =>
          scribe.error(e, s"Exception while copying $source to $tmp")
          throw e
      }

      destination.delete

      def tryRename(i: Int): Boolean = {
        val success = tmp.renameTo(destination)
        if (success) success
        else if (i > 0) {
          scribe.warn(
            s"can't rename file $tmp to $destination. $tmp canRead : ${tmp.canRead}. Try $i more times."
          )
          tryRename(i - 1)
        } else {
          scribe.error(
            s"can't rename file $tmp to $destination. $tmp canRead : ${tmp.canRead}"
          )
          false
        }
      }

      val succ = tryRename(3)
      if (succ) {
        tmp.delete

      } else
        throw new RuntimeException(
          s"can't rename file $tmp to $destination. $tmp canRead : ${tmp.canRead}"
        )
    }
  }

  private def assemblePath(path: ManagedFilePath): File = {
    new File(
      basePath.getAbsolutePath + File.separator + path.pathElements
        .mkString(File.separator)
    )
  }

  private def assemblePath(path: ManagedFilePath, str: String): File = {
    new File(
      basePath.getAbsolutePath + File.separator + path.pathElements
        .mkString(File.separator) + str
    )
  }

  def sink(
      path: ProposedManagedFilePath
  ): Pipe[IO, Byte, (Long, Int, ManagedFilePath)] = { (in: Stream[IO, Byte]) =>
    val tmp = createLocalTempFile()
    Stream.eval(
      in.through(
        fs2.io.file
          .Files[IO]
          .writeAll(fs2.io.file.Path.fromNioPath(tmp.toPath()))
      ).compile
        .drain
        .flatMap { _ =>
          importFile(tmp, path, canMove = true)
            .guarantee(IO.interruptible { if (tmp.exists) { tmp.delete } })
            .map(x => (x._1, x._2, x._3))
        }
    )
  }

  private def checkContentEquality(file1: File, file2: File): IO[Boolean] =
    if (config.folderFileStorageCompleteFileCheck) {
      val s1 = fs2.io.file
        .Files[IO]
        .readAll(fs2.io.file.Path.fromNioPath(file1.toPath))
      val s2 = fs2.io.file
        .Files[IO]
        .readAll(fs2.io.file.Path.fromNioPath(file2.toPath))

      s1.chunks
        .zip(s2.chunks)
        .map { case (c1, c2) => c1.equals(c2) }
        .takeThrough(identity)
        .compile
        .last
        .map(_.isEmpty)

    } else {
      val a1 = FolderFileStorage.getContentHashOfFile(
        file1,
        config.skipContentHashCreationUponImport
      )
      val a2 = FolderFileStorage
        .getContentHashOfFile(file2, config.skipContentHashCreationUponImport)

      val lengthMatch = IO.interruptible(file1.length === file2.length)

      lengthMatch.flatMap { lengthMatch =>
        if (lengthMatch) IO.both(a1, a2).map(x => x._1 === x._2)
        else IO.pure(false)
      }

    }

  override def importFile(
      file: File,
      proposed: ProposedManagedFilePath,
      canMove: Boolean
  ): IO[(Long, Int, ManagedFilePath)] = {
    def copy(dest: File) =
      copyFile(file, dest, canMove)

    val managed = proposed.toManaged
    FolderFileStorage
      .getContentHashOfFile(
        file,
        config.skipContentHashCreationUponImport
      )
      .flatMap { hash =>
        IO.blocking((file.length(), assemblePath(managed).canRead)).flatMap {
          case (size, canRead) =>
            scribe.debug(s"Importing file $file under name $proposed")

            if (canRead) {
              val finalFile = assemblePath(managed)
              scribe.debug(
                s"Found a file already in storage with the same name ($finalFile). Check for equality."
              )
              if (finalFile === file) IO.pure((size, hash, managed))
              else
                checkContentEquality(finalFile, file).flatMap { contentEquals =>
                  if (contentEquals)
                    IO.pure((size, hash, managed))
                  else
                    IO {
                      scribe.info(
                        s"Equality check failed for a file at the same path. Importing file. $file to $finalFile"
                      )

                      if (!config.allowOverwrite) {
                        def candidates(i: Int, past: List[File]): List[File] = {
                          val candidate = assemblePath(managed, ".old." + i)
                          if (candidate.canRead)
                            candidates(i + 1, candidate :: past)
                          else past
                        }

                        def parseVersion(f: File): Int =
                          f.getName.split("\\.").last.toInt

                        val oldFiles = candidates(0, Nil)

                        scribe.debug(s"Moving $oldFiles away.")

                        oldFiles
                          .map(f => (parseVersion(f) + 1, f))
                          .sortBy(_._1)
                          .reverse
                          .foreach { case (newversion, f) =>
                            java.nio.file.Files.move(
                              f.toPath(),
                              assemblePath(managed, ".old." + newversion)
                                .toPath()
                            )
                          }

                        java.nio.file.Files.move(
                          finalFile.toPath(),
                          assemblePath(managed, ".old.0").toPath()
                        )
                      }

                      copy(finalFile)
                      (size, hash, managed)
                    }
                }
            } else
              IO.blocking {
                copy(assemblePath(managed))
                (size, hash, managed)
              }
        }
      }
  }

  def uri(mp: ManagedFilePath) = IO.pure {
    // throw new RuntimeException("URI not supported")
    val path = assemblePath(mp).toURI.toString
    Uri(path)
  }

}
