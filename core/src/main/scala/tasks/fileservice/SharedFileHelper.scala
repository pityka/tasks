/*
 * The MIT License
 *
 * Copyright (c) 2017 Istvan Bartha
 *
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

import akka.actor._

import java.io.File

import tasks.util._
import tasks.util.config._
import tasks.queue._

import cats.effect.kernel.Resource
import cats.effect.IO
import fs2.Stream
import fs2.Chunk

private[tasks] object SharedFileHelper {

  def getByName(name: String, retrieveSizeAndHash: Boolean)(implicit
      service: FileServiceComponent,
      prefix: FileServicePrefix
  ): IO[Option[SharedFile]] =
    recreateFromManagedPath(prefix.propose(name).toManaged, retrieveSizeAndHash)

  private[tasks] def createForTesting(name: String) =
    new SharedFile(ManagedFilePath(Vector(name)), 0, 0)
  private[tasks] def createForTesting(name: String, size: Long, hash: Int) =
    new SharedFile(ManagedFilePath(Vector(name)), size, hash)

  def create(size: Long, hash: Int, path: ManagedFilePath): SharedFile =
    new SharedFile(path, size, hash)

  def create(path: RemoteFilePath, storage: RemoteFileStorage): IO[SharedFile] =
    storage.getSizeAndHash(path).map { case (size, hash) =>
      new SharedFile(path, size, hash)
    }

  val isLocal = (f: File) => f.canRead

  private def getStreamToManagedPath(path: ManagedFilePath, fromOffset: Long)(
      implicit service: FileServiceComponent
  ) =
    service.storage.stream(path, fromOffset: Long)

  def stream(sf: SharedFile, fromOffset: Long)(implicit
      service: FileServiceComponent
  ): fs2.Stream[IO, Byte] =
    sf.path match {
      case path: RemoteFilePath =>
        service.remote
          .stream(path, fromOffset)
      case path: ManagedFilePath =>
        getStreamToManagedPath(path, fromOffset)
    }

  def getPathToFile(sf: SharedFile)(implicit
      service: FileServiceComponent,
      nlc: NodeLocalCache.State
  ): Resource[IO, File] =
    tasks.util.concurrent.NodeLocalCache
      .offer("fs::" + sf, getPathToFileUnCachedInResource(sf), nlc)
      .map(_.asInstanceOf[File])

  private def getPathToFileUnCachedInResource(sf: SharedFile)(implicit
      service: FileServiceComponent
  ): Resource[IO, File] =
    sf.path match {
      case p: RemoteFilePath => service.remote.exportFile(p)
      case p: ManagedFilePath =>
        service.storage.exportFile(p)

    }

  def isAccessible(sf: SharedFile, verifyContent: Boolean)(implicit
      service: FileServiceComponent
  ): IO[Boolean] =
    sf.path match {
      case x: RemoteFilePath =>
        val byteSize = if (verifyContent) sf.byteSize else -1L
        service.remote.contains(x, byteSize, sf.hash)
      case path: ManagedFilePath =>
        val byteSize = if (verifyContent) sf.byteSize else -1L
        service.storage.contains(path, byteSize, sf.hash)

    }

  def getUri(
      sf: SharedFile
  )(implicit service: FileServiceComponent): IO[Uri] =
    sf.path match {
      case RemoteFilePath(path) => IO.pure(path)
      case path: ManagedFilePath =>
        service.storage.uri(path)

    }

  def delete(
      sf: SharedFile
  )(implicit service: FileServiceComponent): IO[Boolean] =
    sf.path match {
      case RemoteFilePath(_) => IO.pure(false)
      case path: ManagedFilePath => {
        service.storage.delete(path, sf.byteSize, sf.hash)

      }
    }

  def saveHistory(sf: SharedFile, historyContext: HistoryContext)(implicit
      prefix: FileServicePrefix,
      service: FileServiceComponent,
      config: TasksConfig
  ): IO[Unit] = {
    historyContext match {
      case NoHistory => IO.unit
      case ctx: HistoryContextImpl if config.writeFileHistories =>
        (IO {
          val history = History(sf, Some(ctx))
          val serialized =
            com.github.plokhotnyuk.jsoniter_scala.core.writeToArray(history)

          implicit val hctx = NoHistory

          createFromStream(
            Stream.chunk(Chunk.array(serialized)),
            sf.name + ".history"
          ).map(_ => ())

        }).flatten
      case _ => IO.unit
    }

  }

  def getHistory(sf: SharedFile)(implicit
      service: FileServiceComponent
  ): IO[History] = {
    sf.path match {
      case _: RemoteFilePath => IO.pure(History(sf, None))
      case managed: ManagedFilePath =>
        IO {
          val historyManagedPath = ManagedFilePath(
            managed.pathElements.dropRight(1) :+ managed.name + ".history"
          )
          scribe.debug("Decoding history of " + sf)

          def readFile =
            getStreamToManagedPath(historyManagedPath, fromOffset = 0L)
              .take(1024 * 1024 * 20L)
              .compile
              .foldChunks(Chunk.empty[Byte])(_ ++ _)
              .map(_.toArray)
              .map { byteArray =>
                if (byteArray.length >= 1024 * 1024 * 20) {
                  scribe.warn("Truncating history due to file size.")
                  History(sf, None)
                } else {

                  com.github.plokhotnyuk.jsoniter_scala.core
                    .readFromArray[History](byteArray)

                }
              }

          for {
            fileIsPresent <- recreateFromManagedPath(historyManagedPath, false)
            history <-
              if (fileIsPresent.isDefined) readFile
              else IO.pure(History(sf, None))
          } yield history
        }.flatten

    }
  }

  def recreateFromManagedPath(
      managed: ManagedFilePath,
      retrieveSizeAndHash: Boolean
  )(implicit service: FileServiceComponent): IO[Option[SharedFile]] =
    service.storage.contains(managed, retrieveSizeAndHash)

  def createFromFile(file: File, name: String, deleteFile: Boolean)(implicit
      prefix: FileServicePrefix,
      service: FileServiceComponent,
      config: TasksConfig,
      historyContext: HistoryContext
  ): IO[SharedFile] = {
    val sharedFile = {
      val proposedPath = prefix.propose(name)
      service.storage.importFile(file, proposedPath).map { f =>
        if (deleteFile) {
          file.delete
        }
        SharedFileHelper.create(f._1, f._2, f._3)
      }
    }

    for {
      sf <- sharedFile
      _ <- saveHistory(sf, historyContext)
    } yield sf
  }

  def sink(name: String)(implicit
      prefix: FileServicePrefix,
      service: FileServiceComponent,
      config: TasksConfig,
      historyContext: HistoryContext
  ): fs2.Pipe[IO, Byte, SharedFile] = { (in: Stream[IO, Byte]) =>
    service.storage.sink(prefix.propose(name))(in).evalMap {
      case (size, hash, path) =>
        val sf = SharedFileHelper.create(size, hash, path)
        saveHistory(sf, historyContext).map(_ => sf)

    }
  }

  def createFromStream(stream: Stream[IO, Byte], name: String)(implicit
      prefix: FileServicePrefix,
      service: FileServiceComponent,
      config: TasksConfig,
      historyContext: HistoryContext
  ): IO[SharedFile] = {

    val s = sink(name)
    val sharedFile = stream.through(s).compile.last.map(_.get)

    for {
      sf <- sharedFile
      _ <- saveHistory(sf, historyContext)
    } yield sf
  }

  def createFromFolder(parallelism: Int)(callback: File => List[File])(implicit
      prefix: FileServicePrefix,
      service: FileServiceComponent,
      context: ActorRefFactory,
      config: TasksConfig,
      historyContext: HistoryContext
  ): IO[List[SharedFile]] = {
    val directory = service.storage
      .sharedFolder(prefix.list)
      .map(
        _.getOrElse(
          throw new RuntimeException(
            "Storage does not support shared folders"
          )
        )
      )
    directory.flatMap { directory =>
      val files = callback(directory)
      val directoryPath = directory.getAbsolutePath
      IO.parSequenceN(parallelism)(files.map { file =>
        assert(file.getAbsolutePath.startsWith(directoryPath))
        createFromFile(
          file,
          file.getAbsolutePath.drop(directoryPath.size).stripPrefix("/"),
          deleteFile = false
        )
      })
    }
  }

}
