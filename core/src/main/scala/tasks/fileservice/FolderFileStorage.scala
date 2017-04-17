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

import akka.actor.{Actor, PoisonPill, ActorRef, Props, ActorRefFactory}
import akka.actor.Actor._
import akka.pattern.ask
import akka.pattern.pipe
import akka.stream._
import scala.concurrent.{Future, ExecutionContext}
import java.lang.Class
import java.io.{File, InputStream, FileInputStream, BufferedInputStream}
import scala.concurrent.duration._
import java.util.concurrent.{TimeUnit, ScheduledFuture}
import java.nio.channels.{WritableByteChannel, ReadableByteChannel}
import tasks.util._
import scala.util._
import com.google.common.hash._
import scala.concurrent._
import scala.util._
import tasks.util.eq._
import akka.stream.scaladsl._
import akka.util._

object FolderFileStorage {

  private def getContentHash(file: File): Int = {
    val checkedSize = 1024 * 256
    val buffer = Array.fill[Byte](checkedSize)(0)
    openFileInputStream(file) { is =>
      FileStorage.getContentHash(is)
    }
  }

  private def fileIsRelativeToNonLocalFileSystem(f: File): Boolean = {
    val nonLocalFileSystemsCanonical =
      tasks.util.config.global.nonLocalFileSystems.map(_.getCanonicalPath)

    val canonical = f.getCanonicalFile

    def getParents(f: File, p: List[File]): List[File] =
      if (f == null) p
      else getParents(f.getParentFile, f :: p)

    val canonicalParents = getParents(canonical, Nil).map(_.getCanonicalPath)

    (nonLocalFileSystemsCanonical).exists(path =>
          canonicalParents.contains(path))
  }
}

class FolderFileStorage(val basePath: File,
                        val centralized: Boolean,
                        val extendedPaths: List[File] = Nil)(
    implicit mat: ActorMaterializer,
    ec: ExecutionContext)
    extends ManagedFileStorage {

  if (basePath.exists && !basePath.isDirectory)
    throw new IllegalArgumentException(s"$basePath exists and not a folder")
  else if (!basePath.exists) basePath.mkdirs

  if (!basePath.isDirectory)
    throw new RuntimeException(s"Could not create $basePath")

  override def toString =
    s"FolderFileStorage(basePath=$basePath, centralized=$centralized, extendedPaths=$extendedPaths)"

  private val canonicalExtendedPaths = extendedPaths.map(_.getCanonicalPath)
  private val canonicalBasePath = basePath.getCanonicalPath

  private def fileIsRelativeToBaseOrExtended(f: File): Boolean = {
    val canonical = f.getCanonicalFile

    def getParents(f: File, p: List[File]): List[File] =
      if (f == null) p
      else getParents(f.getParentFile, f :: p)

    val canonicalParents = getParents(canonical, Nil).map(_.getCanonicalPath)

    (canonicalBasePath :: canonicalExtendedPaths).exists(path =>
          canonicalParents.contains(path))

  }

  def contains(path: ManagedFilePath, size: Long, hash: Int): Future[Boolean] =
    Future.successful {
      val f = assemblePath(path)
      f.canRead && (size < 0 || (f.length === size && (tasks.util.config.global.skipContentHashVerificationAfterCache || FolderFileStorage
                    .getContentHash(f) === hash)))
    }

  def createSource(path: ManagedFilePath): Source[ByteString, _] =
    FileIO.fromPath(assemblePath(path).toPath)

  def createStream(path: ManagedFilePath): Try[InputStream] =
    Try(new FileInputStream(assemblePath(path)))

  def exportFile(path: ManagedFilePath): Future[File] =
    Future.successful(assemblePath(path))

  private def copyFile(source: File, destination: File): Unit = {
    val parentFolder = destination.getParentFile
    parentFolder.mkdirs
    val tmp = new File(parentFolder, destination.getName + ".tmp")

    com.google.common.io.Files.copy(source, tmp)

    destination.delete
    val succ = tmp.renameTo(destination)
    if (succ) {
      tmp.delete

    } else throw new RuntimeException("can't rename file" + destination)
  }

  private def assemblePath(path: ManagedFilePath): File = {
    new File(
        basePath.getAbsolutePath + File.separator + path.pathElements.mkString(
            File.separator))
  }

  private def assemblePath(path: ManagedFilePath, str: String): File = {
    new File(
        basePath.getAbsolutePath + File.separator + path.pathElements.mkString(
            File.separator) + str)
  }

  def importSource(s: Source[ByteString, _], path: ProposedManagedFilePath)(
      implicit mat: Materializer): Future[(Long, Int, ManagedFilePath)] = {
    val tmp = TempFile.createTempFile("foldertmp")
    s.runWith(FileIO.toPath(tmp.toPath)).flatMap { _ =>
      val r = importFile(tmp, path)
      tmp.delete
      r.map(x => (x._1, x._2, x._4))
    }

  }

  def importFile(file: File, proposed: ProposedManagedFilePath)
    : Future[(Long, Int, File, ManagedFilePath)] =
    Future.successful({

      val size = file.length
      val hash = FolderFileStorage.getContentHash(file)
      val managed = proposed.toManaged

      if (fileIsRelativeToBaseOrExtended(file)) (size, hash, file, managed)
      else if (assemblePath(managed).canRead) {
        val finalFile = assemblePath(managed)
        if (com.google.common.io.Files.equal(finalFile, file))
          (size, hash, finalFile, managed)
        else {

          def candidates(i: Int, past: List[File]): List[File] = {
            val candidate = assemblePath(managed, ".old." + i)
            if (candidate.canRead) candidates(i + 1, candidate :: past)
            else past
          }

          candidates(0, Nil)
            .map(f => (f.getName.drop(managed.name.size).drop(5).toInt + 1, f))
            .sortBy(_._1)
            .reverse
            .foreach {
              case (newversion, f) =>
                com.google.common.io.Files
                  .move(f, assemblePath(managed, ".old." + newversion))
            }

          com.google.common.io.Files
            .move(finalFile, assemblePath(managed, ".old.0"))

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
    import scala.collection.JavaConversions._
    val stream =
      java.nio.file.Files.newDirectoryStream(basePath.toPath, pattern)
    try {
      stream.toList.filter(_.toFile.isFile).map { path =>
        val file = path.toFile
        val l = file.length
        val h = FolderFileStorage.getContentHash(file)
        new SharedFile(ManagedFilePath(
                           basePath.toPath
                             .relativize(path)
                             .iterator
                             .map(_.toString)
                             .toVector),
                       l,
                       h)
      }
    } catch {
      case x: Throwable => throw x
    } finally {
      stream.close
    }

  }

}
