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

import scala.concurrent.{Future, ExecutionContext}
import java.io.File
import tasks.util._
import scala.concurrent._
import tasks.util.eq._
import tasks.util.config._
import akka.stream.scaladsl._
import akka.util._

object FolderFileStorage {

  private[tasks] def getContentHash(file: File): Int = {
    openFileInputStream(file) { is =>
      FileStorage.getContentHash(is)
    }
  }

}

class FolderFileStorage(val basePath: File)(
    implicit
    ec: ExecutionContext,
    config: TasksConfig,
    as: akka.actor.ActorSystem
) extends ManagedFileStorage {

  if (basePath.exists && !basePath.isDirectory)
    throw new IllegalArgumentException(s"$basePath exists and not a folder")
  else if (!basePath.exists) basePath.mkdirs

  if (!basePath.isDirectory)
    throw new RuntimeException(s"Could not create $basePath")

  val logger = akka.event.Logging(as, getClass)

  override def toString =
    s"FolderFileStorage(basePath=$basePath)"

  private val canonicalBasePath = basePath.getCanonicalPath

  private def fileIsRelativeToBase(f: File): Boolean = {
    val canonical = f.getCanonicalFile

    def getParents(f: File, p: List[File]): List[File] =
      if (f == null) p
      else getParents(f.getParentFile, f :: p)

    val canonicalParents = getParents(canonical, Nil).map(_.getCanonicalPath)

    (canonicalBasePath :: Nil).exists(path => canonicalParents.contains(path))

  }

  def delete(mp: ManagedFilePath, expectedSize: Long, expectedHash: Int) =
    if (config.allowDeletion) {
      val file = assemblePath(mp)
      val sizeOnDiskNow = file.length
      val sizeMatch = sizeOnDiskNow == expectedSize
      val canRead = file.canRead
      def contentMatch =
        canRead && FolderFileStorage.getContentHash(file) === expectedHash
      val canDelete = canRead && (expectedSize < 0 || (sizeMatch && contentMatch))
      if (canDelete) {
        file.delete
        logger.warning(s"File deleted $file $mp")
      } else {
        logger.warning(
          s"Not deleting file because its size or hash is different than expectation. $file $mp $sizeMatch $contentMatch"
        )
        Future.successful(false)
      }
      Future.successful(true)
    } else {
      logger.warning(s"File deletion disabled. Would have deleted $mp")
      Future.successful(false)
    }

  def sharedFolder(prefix: Seq[String]): Option[File] = {
    val folder = new File(
      basePath.getAbsolutePath + File.separator + prefix
        .mkString(File.separator)
    )
    folder.mkdirs
    Some(folder)
  }

  def contains(path: ManagedFilePath, size: Long, hash: Int): Future[Boolean] =
    Future.successful {
      val f = assemblePath(path)
      val canRead = f.canRead
      val sizeOnDiskNow = f.length
      val sizeMatch = sizeOnDiskNow === size
      def contentMatch =
        (config.skipContentHashVerificationAfterCache || (canRead && FolderFileStorage
          .getContentHash(f) === hash))
      val pass = canRead && (size < 0 || (sizeMatch && contentMatch))

      if (!pass) {
        logger.debug(
          s"$this does not contain $path due to: canRead:$canRead, sizeMatch:$sizeMatch   (sizeOnDisk:$sizeOnDiskNow vs expected:$size), contentMatch:$contentMatch. FileOnDisk: $f "
        )
      }

      pass
    }

  def contains(
      path: ManagedFilePath,
      retrieveSizeAndHash: Boolean
  ): Future[Option[SharedFile]] =
    Future.successful {
      val f = assemblePath(path)
      val canRead = f.canRead
      val pass = canRead

      if (!pass) {
        logger.debug(s"$this does not contain $path due to: canRead:$canRead")
        None
      } else {
        if (retrieveSizeAndHash) {
          val size = f.length
          val hash = FolderFileStorage.getContentHash((f))
          Some(SharedFileHelper.create(size, hash, path))
        } else Some(SharedFileHelper.create(size = -1L, hash = 0, path))
      }

    }

  def createSource(
      path: ManagedFilePath,
      fromOffset: Long
  ): Source[ByteString, _] =
    Source.lazySource(
      () =>
        FileIO.fromPath(
          assemblePath(path).toPath,
          chunkSize = 8192,
          startPosition = fromOffset
        )
    )

  def exportFile(path: ManagedFilePath): Future[File] = {
    val file = assemblePath(path)
    if (file.canRead) Future.successful(file)
    else {
      Future.failed(
        new java.nio.file.NoSuchFileException(
          file.getAbsolutePath + " " + System.nanoTime
        )
      )
    }
  }
  private def copyFile(source: File, destination: File): Unit = {
    val parentFolder = destination.getParentFile
    parentFolder.mkdirs
    val tmp = new File(parentFolder, destination.getName + ".tmp")

    try {
      com.google.common.io.Files.copy(source, tmp)
    } catch {
      case e: java.io.IOException =>
        logger.error(e, s"Exception while copying $source to $tmp")
        throw e
    }

    destination.delete

    def tryRename(i: Int): Boolean = {
      val success = tmp.renameTo(destination)
      if (success) success
      else if (i > 0) {
        logger.warning(
          s"can't rename file $tmp to $destination. $tmp canRead : ${tmp.canRead}. Try $i more times."
        )
        tryRename(i - 1)
      } else {
        logger.error(
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
  ): Sink[ByteString, Future[(Long, Int, ManagedFilePath)]] = {
    val createSink = () => {
      val tmp = TempFile.createTempFile("foldertmp")
      Future.successful(
        FileIO
          .toPath(tmp.toPath)
          .mapMaterializedValue(_.flatMap { _ =>
            val r = importFile(tmp, path)
            tmp.delete
            r.map(x => (x._1, x._2, x._4))
          })
      )
    }

    Sink
      .lazyFutureSink(createSink)
      .mapMaterializedValue(_.flatten.recoverWith {
        case _ =>
          // empty file, upstream terminated without emitting an element
          val tmp = TempFile.createTempFile("foldertmp")
          val r = importFile(tmp, path)
          r.map(x => { tmp.delete; (x._1, x._2, x._4) })
      })
  }

  private def checkContentEquality(file1: File, file2: File) =
    if (config.folderFileStorageCompleteFileCheck)
      com.google.common.io.Files.equal(file1, file2)
    else
      file1.length == file2.length && FolderFileStorage.getContentHash(file1) == FolderFileStorage
        .getContentHash(file2)

  def importFile(
      file: File,
      proposed: ProposedManagedFilePath
  ): Future[(Long, Int, File, ManagedFilePath)] =
    Future.successful({
      logger.debug(s"Importing file $file under name $proposed")
      val size = file.length
      val hash = FolderFileStorage.getContentHash(file)
      val managed = proposed.toManaged

      if (fileIsRelativeToBase(file)) {
        val locationAsManagedFilePath = {
          val relativeToBase =
            file.getAbsolutePath.stripPrefix(canonicalBasePath)
          val elements = relativeToBase.split('/').toVector.filter(_.nonEmpty)
          ManagedFilePath(elements)
        }
        (size, hash, file, locationAsManagedFilePath)
      } else if (assemblePath(managed).canRead) {
        val finalFile = assemblePath(managed)
        logger.debug(
          s"Found a file already in storage with the same name ($finalFile). Check for equality."
        )
        if (checkContentEquality(finalFile, file))
          (size, hash, finalFile, managed)
        else {
          logger.debug(s"Equality failed. Importing file. $file to $finalFile")

          if (!config.allowOverwrite) {
            def candidates(i: Int, past: List[File]): List[File] = {
              val candidate = assemblePath(managed, ".old." + i)
              if (candidate.canRead) candidates(i + 1, candidate :: past)
              else past
            }

            def parseVersion(f: File): Int =
              f.getName.split("\\.").last.toInt

            val oldFiles = candidates(0, Nil)

            logger.debug(s"Moving $oldFiles away.")

            oldFiles
              .map(f => (parseVersion(f) + 1, f))
              .sortBy(_._1)
              .reverse
              .foreach {
                case (newversion, f) =>
                  com.google.common.io.Files
                    .move(f, assemblePath(managed, ".old." + newversion))
              }

            com.google.common.io.Files
              .move(finalFile, assemblePath(managed, ".old.0"))
          }

          copyFile(file, finalFile)
          (size, hash, finalFile, managed)
        }

      } else {
        copyFile(file, assemblePath(managed))
        (size, hash, assemblePath(managed), managed)
      }
    })

  def uri(mp: ManagedFilePath) = {
    // throw new RuntimeException("URI not supported")
    val path = assemblePath(mp).toURI.toString
    Uri(path)
  }

  def list(pattern: String): List[SharedFile] = {
    import scala.collection.JavaConverters._
    val stream =
      java.nio.file.Files.newDirectoryStream(basePath.toPath, pattern)
    try {
      stream.asScala.toList.filter(_.toFile.isFile).map { path =>
        val file = path.toFile
        val l = file.length
        val h = FolderFileStorage.getContentHash(file)
        new SharedFile(
          ManagedFilePath(
            basePath.toPath
              .relativize(path)
              .asScala
              .iterator
              .map(_.toString)
              .toVector
          ),
          l,
          h
        )
      }
    } catch {
      case x: Throwable => throw x
    } finally {
      stream.close
    }

  }

}
