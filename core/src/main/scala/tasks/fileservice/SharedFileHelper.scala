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
import akka.stream.scaladsl._
import akka.stream._
import akka.util._

import scala.concurrent._

import java.io.File

import tasks.util._
import tasks.util.config._
import tasks.queue._

import com.typesafe.scalalogging.StrictLogging
import akka.NotUsed
import cats.effect.kernel.Resource
import cats.effect.IO

private[tasks] object SharedFileHelper extends StrictLogging {

  def getByName(name: String, retrieveSizeAndHash: Boolean)(implicit
      service: FileServiceComponent,
      prefix: FileServicePrefix
  ): Future[Option[SharedFile]] =
    recreateFromManagedPath(prefix.propose(name).toManaged, retrieveSizeAndHash)

  private[tasks] def createForTesting(name: String) =
    new SharedFile(ManagedFilePath(Vector(name)), 0, 0)
  private[tasks] def createForTesting(name: String, size: Long, hash: Int) =
    new SharedFile(ManagedFilePath(Vector(name)), size, hash)

  def create(size: Long, hash: Int, path: ManagedFilePath): SharedFile =
    new SharedFile(path, size, hash)

  def create(path: RemoteFilePath, storage: RemoteFileStorage)(implicit
      ec: ExecutionContext
  ): Future[SharedFile] =
    storage.getSizeAndHash(path).map { case (size, hash) =>
      new SharedFile(path, size, hash)
    }

  val isLocal = (f: File) => f.canRead

  private def getSourceToManagedPath(path: ManagedFilePath, fromOffset: Long)(
      implicit
      service: FileServiceComponent,
  ) =
    service.storage.createSource(path, fromOffset: Long)

  def getSourceToFile(sf: SharedFile, fromOffset: Long)(implicit
      service: FileServiceComponent,
  ): Source[ByteString, NotUsed] =
    sf.path match {
      case path: RemoteFilePath =>
        service.remote
          .createSource(path, fromOffset)
          .mapMaterializedValue(_ => NotUsed)
      case path: ManagedFilePath =>
        getSourceToManagedPath(path, fromOffset).mapMaterializedValue(_ =>
          NotUsed
        )
    }

  def getPathToFile(sf: SharedFile)(implicit
      service: FileServiceComponent,
      nlc: NodeLocalCache.State,
  ): Resource[IO, File] =
    tasks.util.concurrent.NodeLocalCache
      .offer("fs::" + sf, getPathToFileUnCachedInResource(sf), nlc)
      .map(_.asInstanceOf[File])

  private def getPathToFileUnCachedInResource(sf: SharedFile)(implicit
      service: FileServiceComponent,
  ): Resource[IO, File] =
    sf.path match {
      case p: RemoteFilePath => service.remote.exportFile(p)
      case p: ManagedFilePath =>
        service.storage.exportFile(p)

    }

  def isAccessible(sf: SharedFile, verifyContent: Boolean)(implicit
      service: FileServiceComponent
  ): Future[Boolean] =
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
  )(implicit service: FileServiceComponent): Future[Uri] =
    sf.path match {
      case RemoteFilePath(path) => Future.successful(path)
      case path: ManagedFilePath =>
        service.storage.uri(path)

    }

  def delete(
      sf: SharedFile
  )(implicit service: FileServiceComponent): Future[Boolean] =
    sf.path match {
      case RemoteFilePath(_) => Future.successful(false)
      case path: ManagedFilePath => {
        // if (service.storage.isDefined)
        service.storage.delete(path, sf.byteSize, sf.hash)
        // else {
        //   implicit val timout = akka.util.Timeout(1441 minutes)
        //   (service.actor ? Delete(path, sf.byteSize, sf.hash))
        //     .asInstanceOf[Future[Boolean]]
        // }
      }
    }

  def saveHistory(sf: SharedFile, historyContext: HistoryContext)(implicit
      prefix: FileServicePrefix,
      ec: ExecutionContext,
      service: FileServiceComponent,
      context: ActorRefFactory,
      config: TasksConfig,
      mat: Materializer
  ): Future[Unit] = {
    historyContext match {
      case NoHistory => Future.successful(())
      case ctx: HistoryContextImpl if config.writeFileHistories =>
        val history = History(sf, Some(ctx))
        val serialized =
          com.github.plokhotnyuk.jsoniter_scala.core.writeToArray(history)

        implicit val hctx = NoHistory
        createFromSource(
          Source.single(ByteString(serialized)),
          sf.name + ".history"
        ).map(_ => ())
      case _ => Future.successful(())
    }

  }

  def getHistory(sf: SharedFile)(implicit
      service: FileServiceComponent,
      ec: ExecutionContext,
      mat: Materializer
  ): Future[History] = {
    sf.path match {
      case _: RemoteFilePath => Future.successful(History(sf, None))
      case managed: ManagedFilePath =>
        val historyManagedPath = ManagedFilePath(
          managed.pathElements.dropRight(1) :+ managed.name + ".history"
        )
        logger.debug("Decoding history of " + sf)

        def readFile =
          getSourceToManagedPath(historyManagedPath, fromOffset = 0L)
            .take(1024 * 1024 * 20L)
            .runFold(ByteString.empty)(_ ++ _)
            .map(_.toArray)
            .map { byteArray =>
              if (byteArray.length >= 1024 * 1024 * 20) {
                logger.warn("Truncating history due to file size.")
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
            else Future.successful(History(sf, None))
        } yield history

    }
  }

  def recreateFromManagedPath(
      managed: ManagedFilePath,
      retrieveSizeAndHash: Boolean
  )(implicit service: FileServiceComponent): Future[Option[SharedFile]] =
    service.storage.contains(managed, retrieveSizeAndHash)

  def createFromFile(file: File, name: String, deleteFile: Boolean)(implicit
      prefix: FileServicePrefix,
      ec: ExecutionContext,
      service: FileServiceComponent,
      context: ActorRefFactory,
      config: TasksConfig,
      historyContext: HistoryContext,
      mat: Materializer
  ) = {
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
      ec: ExecutionContext,
      service: FileServiceComponent,
      context: ActorRefFactory,
      mat: Materializer,
      config: TasksConfig,
      historyContext: HistoryContext
  ) = {
    service.storage.sink(prefix.propose(name)).mapMaterializedValue {
      futureOfPath =>
        val sf = futureOfPath.map { case (size, hash, path) =>
          SharedFileHelper.create(size, hash, path)
        }
        for {
          sf <- sf
          _ <- saveHistory(sf, historyContext)
        } yield sf
    }
  }

  def createFromSource(source: Source[ByteString, _], name: String)(implicit
      prefix: FileServicePrefix,
      ec: ExecutionContext,
      service: FileServiceComponent,
      context: ActorRefFactory,
      mat: Materializer,
      config: TasksConfig,
      historyContext: HistoryContext
  ) = {

    val sharedFile =
      source.runWith(sink(name))

    for {
      sf <- sharedFile
      _ <- saveHistory(sf, historyContext)
    } yield sf
  }

  def createFromFolder(callback: File => List[File])(implicit
      prefix: FileServicePrefix,
      ec: ExecutionContext,
      service: FileServiceComponent,
      context: ActorRefFactory,
      config: TasksConfig,
      historyContext: HistoryContext,
      mat: Materializer
  ) =
    {
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
        Future.sequence(files.map { file =>
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
