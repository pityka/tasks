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
import akka.pattern.ask
import akka.stream.scaladsl._
import akka.stream._
import akka.util._

import scala.concurrent.duration._
import scala.concurrent._
import scala.util._

import java.io.File

import tasks.util._
import tasks.util.config._
import tasks.queue._
import tasks.wire._

import com.typesafe.scalalogging.StrictLogging

private[tasks] object SharedFileHelper extends StrictLogging {

  def getByNameUnchecked(name: String)(
      implicit service: FileServiceComponent,
      context: ActorRefFactory,
      nlc: NodeLocalCacheActor,
      prefix: FileServicePrefix,
      ec: ExecutionContext): Future[Option[SharedFile]] = {
    val sf = new SharedFile(prefix.propose(name).toManaged, -1L, 0)
    getPathToFile(sf)
      .map { _ =>
        Some(sf)
      }
      .recover {
        case _: java.nio.file.NoSuchFileException =>
          None
      }
  }
  def createForTesting(name: String) =
    new SharedFile(ManagedFilePath(Vector(name)), 0, 0)
  def createForTesting(name: String, size: Long, hash: Int) =
    new SharedFile(ManagedFilePath(Vector(name)), size, hash)

  def create(size: Long, hash: Int, path: ManagedFilePath): SharedFile =
    new SharedFile(path, size, hash)

  def create(path: RemoteFilePath, storage: RemoteFileStorage)(
      implicit ec: ExecutionContext): Future[SharedFile] =
    storage.getSizeAndHash(path).map {
      case (size, hash) =>
        new SharedFile(path, size, hash)
    }

  val isLocal = (f: File) => f.canRead

  private def getSourceToManagedPath(path: ManagedFilePath)(
      implicit service: FileServiceComponent,
      context: ActorRefFactory,
      ec: ExecutionContext) =
    if (service.storage.isDefined) {
      service.storage.get.createSource(path)
    } else {
      val serviceactor = service.actor
      implicit val timout = akka.util.Timeout(1441 minutes)
      val ac = context.actorOf(
        Props(new FileUserSource(path, serviceactor, isLocal))
          .withDispatcher("fileuser-dispatcher"))

      val f = (ac ? WaitingForPath)
        .asInstanceOf[Future[Try[Source[ByteString, _]]]]

      f onComplete {
        case _ => ac ! PoisonPill
      }

      val f2 = f map (_ match {
        case Success(r) => r
        case Failure(e) =>
          throw new RuntimeException("getSourceToFile failed. " + path, e)
      })

      Source.fromFuture(f2).flatMapConcat(x => x)
    }

  def getSourceToFile(sf: SharedFile)(
      implicit service: FileServiceComponent,
      context: ActorRefFactory,
      ec: ExecutionContext): Source[ByteString, _] =
    sf.path match {
      case path: RemoteFilePath  => service.remote.createSource(path)
      case path: ManagedFilePath => getSourceToManagedPath(path)
    }

  def getPathToFile(sf: SharedFile)(implicit service: FileServiceComponent,
                                    context: ActorRefFactory,
                                    nlc: NodeLocalCacheActor,
                                    ec: ExecutionContext): Future[File] =
    NodeLocalCache
      ._getItemAsync("fs::" + sf, dropAfterSave = false)(
        getPathToFileUncached(sf))
      .recoverWith {
        case _: java.nio.file.NoSuchFileException => getPathToFileUncached(sf)
      }

  private def getPathToFileUncached(sf: SharedFile)(
      implicit service: FileServiceComponent,
      context: ActorRefFactory,
      ec: ExecutionContext): Future[File] =
    sf.path match {
      case p: RemoteFilePath => service.remote.exportFile(p)
      case p: ManagedFilePath =>
        if (service.storage.isDefined) {
          service.storage.get.exportFile(p)
        } else {
          val serviceactor = service.actor
          implicit val timout = akka.util.Timeout(1441 minutes)
          val ac = context.actorOf(
            Props(new FileUser(p, sf.byteSize, sf.hash, serviceactor, isLocal))
              .withDispatcher("fileuser-dispatcher"))

          val f = (ac ? WaitingForPath).asInstanceOf[Future[Try[File]]]
          f onComplete {
            case _ => ac ! PoisonPill
          }

          f map (_ match {
            case Success(r) => r
            case Failure(e) =>
              throw new RuntimeException("getPathToFile failed. " + p, e)
          })
        }
    }

  def isAccessible(sf: SharedFile)(
      implicit service: FileServiceComponent): Future[Boolean] =
    sf.path match {
      case x: RemoteFilePath =>
        service.remote.contains(x, sf.byteSize, sf.hash)
      case path: ManagedFilePath =>
        if (service.storage.isDefined) {
          service.storage.get.contains(path, sf.byteSize, sf.hash)
        } else {
          implicit val timout = akka.util.Timeout(1441 minutes)

          (service.actor ? IsAccessible(path, sf.byteSize, sf.hash))
            .asInstanceOf[Future[Boolean]]
        }

    }

  def isAccessible(managed: ManagedFilePath)(
      implicit service: FileServiceComponent): Future[Boolean] =
    if (service.storage.isDefined) {
      service.storage.get.contains(managed)
    } else {
      implicit val timout = akka.util.Timeout(1441 minutes)

      (service.actor ? IsPathAccessible(managed))
        .asInstanceOf[Future[Boolean]]
    }

  def getUri(sf: SharedFile)(
      implicit service: FileServiceComponent): Future[Uri] =
    sf.path match {
      case RemoteFilePath(path) => Future.successful(path)
      case path: ManagedFilePath => {
        if (service.storage.isDefined)
          Future.successful(service.storage.get.uri(path))
        else {
          implicit val timout = akka.util.Timeout(1441 minutes)
          (service.actor ? GetUri(path)).asInstanceOf[Future[Uri]]
        }
      }
    }

  def delete(sf: SharedFile)(
      implicit service: FileServiceComponent): Future[Boolean] =
    sf.path match {
      case RemoteFilePath(_) => Future.successful(false)
      case path: ManagedFilePath => {
        if (service.storage.isDefined)
          service.storage.get.delete(path)
        else {
          implicit val timout = akka.util.Timeout(1441 minutes)
          (service.actor ? Delete(path)).asInstanceOf[Future[Boolean]]
        }
      }
    }

  def saveHistory(sf: SharedFile, historyContext: HistoryContext)(
      implicit prefix: FileServicePrefix,
      ec: ExecutionContext,
      service: FileServiceComponent,
      context: ActorRefFactory,
      config: TasksConfig,
      mat: Materializer,
      nlc: NodeLocalCacheActor): Future[Unit] = {
    historyContext match {
      case NoHistory => Future.successful(())
      case ctx: HistoryContextImpl =>
        val history = History(sf, Some(ctx.deduplicate))
        val serialized =
          History.encoder.apply(history).noSpaces.getBytes("UTF-8")

        implicit val hctx = NoHistory
        createFromSource(Source.single(ByteString(serialized)),
                         sf.name + ".history").map(_ => ())
    }

  }

  def getHistory(sf: SharedFile)(implicit service: FileServiceComponent,
                                 context: ActorRefFactory,
                                 ec: ExecutionContext,
                                 mat: Materializer): Future[History] = {
    sf.path match {
      case _: RemoteFilePath => Future.successful(History(sf, None))
      case managed: ManagedFilePath =>
        val historyManagedPath = ManagedFilePath(
          managed.pathElements.dropRight(1) :+ managed.name + ".history")
        logger.debug("Decoding history of " + sf)

        def readFile =
          getSourceToManagedPath(historyManagedPath)
            .take(1024 * 1024 * 20L)
            .runFold(ByteString.empty)(_ ++ _)
            .map(_.toArray)
            .map { byteArray =>
              if (byteArray.length >= 1024 * 1024 * 20) {
                logger.warn("Truncating history due to file size.")
                History(sf, None)
              } else {
                val string = new String(byteArray)

                io.circe.parser.decode[History](string) match {
                  case Left(e) =>
                    throw e
                  case Right(history) => history.deduplicate
                }
              }
            }

        for {
          fileIsPresent <- isAccessible(historyManagedPath)
          history <- if (fileIsPresent) readFile
          else Future.successful(History(sf, None))
        } yield history

    }
  }

  def createFromFile(file: File, name: String, deleteFile: Boolean)(
      implicit prefix: FileServicePrefix,
      ec: ExecutionContext,
      service: FileServiceComponent,
      context: ActorRefFactory,
      config: TasksConfig,
      historyContext: HistoryContext,
      mat: Materializer,
      nlc: NodeLocalCacheActor) =
    NodeLocalCache
      ._getItemAsync("fsCreateFromFile::" + prefix.propose(name),
                     dropAfterSave = true) {
        val sharedFile = if (service.storage.isDefined) {
          val proposedPath = prefix.propose(name)
          service.storage.get.importFile(file, proposedPath).map { f =>
            if (deleteFile) {
              file.delete
            }
            SharedFileHelper.create(f._1, f._2, f._4)
          }
        } else {
          val serviceactor = service.actor
          if (!file.canRead) {
            throw new java.io.FileNotFoundException("not found" + file)
          }

          implicit val timout = akka.util.Timeout(1441 minutes)

          val ac = context.actorOf(
            Props(
              new FileSender(file,
                             prefix.propose(name),
                             deleteFile,
                             serviceactor))
              .withDispatcher("filesender-dispatcher"))
          (ac ? WaitingForSharedFile)
            .asInstanceOf[Future[Option[SharedFile]]]
            .map(_.get)
            .andThen { case _ => ac ! PoisonPill }

        }
        for {
          sf <- sharedFile
          _ <- saveHistory(sf, historyContext)
        } yield sf
      }

  def createFromSource(source: Source[ByteString, _], name: String)(
      implicit prefix: FileServicePrefix,
      ec: ExecutionContext,
      service: FileServiceComponent,
      context: ActorRefFactory,
      mat: Materializer,
      config: TasksConfig,
      historyContext: HistoryContext,
      nlc: NodeLocalCacheActor) =
    NodeLocalCache
      ._getItemAsync("fsCreateFromSource::" + prefix.propose(name),
                     dropAfterSave = true) {
        val sharedFile = if (service.storage.isDefined) {
          val proposedPath = prefix.propose(name)
          service.storage.get.importSource(source, proposedPath).map { x =>
            SharedFileHelper.create(x._1, x._2, x._3)
          }
        } else {

          val serviceactor = service.actor

          implicit val timout = akka.util.Timeout(1441 minutes)

          val ac = context.actorOf(
            Props(new SourceSender(source, prefix.propose(name), serviceactor))
              .withDispatcher("filesender-dispatcher"))

          (ac ? WaitingForSharedFile)
            .asInstanceOf[Future[Option[SharedFile]]]
            .map(_.get)
            .andThen { case _ => ac ! PoisonPill }

        }
        for {
          sf <- sharedFile
          _ <- saveHistory(sf, historyContext)
        } yield sf
      }

  def createFromFolder(callback: File => List[File])(
      implicit prefix: FileServicePrefix,
      ec: ExecutionContext,
      service: FileServiceComponent,
      context: ActorRefFactory,
      config: TasksConfig,
      historyContext: HistoryContext,
      mat: Materializer,
      nlc: NodeLocalCacheActor) =
    if (service.storage.isDefined) {

      val directory = service.storage.get
        .sharedFolder(prefix.list)
        .getOrElse(
          throw new RuntimeException("Storage does not support shared folders"))
      val files = callback(directory)
      val directoryPath = directory.getAbsolutePath
      Future.sequence(files.map { file =>
        assert(file.getAbsolutePath.startsWith(directoryPath))
        createFromFile(
          file,
          file.getAbsolutePath.drop(directoryPath.size).stripPrefix("/"),
          deleteFile = false)
      })
    } else {

      val serviceactor = service.actor

      implicit val timout = akka.util.Timeout(1441 minutes)

      (serviceactor ? GetSharedFolder(prefix.list))
        .mapTo[Option[File]]
        .flatMap { maySharedFolder =>
          val directory = maySharedFolder.getOrElse {
            val tmp = TempFile.createTempFile("tmpfolder")
            tmp.delete
            tmp.mkdir
            tmp
          }
          val files = callback(directory)
          Future.sequence(files.map { file =>
            val fileName =
              file.getAbsolutePath
                .drop(directory.getAbsolutePath.size)
                .stripPrefix("/")
            createFromFile(file, fileName, deleteFile = false)
          })
        }

    }

}
