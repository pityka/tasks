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

private[tasks] object SharedFileHelper {

  def getByNameUnchecked(name: String)(
      implicit service: FileServiceComponent,
      context: ActorRefFactory,
      nlc: NodeLocalCacheActor,
      prefix: FileServicePrefix,
      ec: ExecutionContext): Future[SharedFile] = {
    getPathToFile(new SharedFile(prefix.propose(name).toManaged, -1L, 0, None))
      .map { f =>
        new SharedFile(prefix.propose(name).toManaged, -1L, 0, None)
      }

  }

  def createForTesting(name: String) =
    new SharedFile(ManagedFilePath(Vector(name)), 0, 0, None)
  def createForTesting(name: String, size: Long, hash: Int) =
    new SharedFile(ManagedFilePath(Vector(name)), size, hash, None)

  def create(size: Long,
             hash: Int,
             path: ManagedFilePath,
             history: Option[History]): SharedFile =
    new SharedFile(path, size, hash, history)

  def create(path: RemoteFilePath, storage: RemoteFileStorage)(
      implicit ec: ExecutionContext): Future[SharedFile] =
    storage.getSizeAndHash(path).map {
      case (size, hash) =>
        new SharedFile(path, size, hash, None)
    }

  val isLocal = (f: File) => f.canRead

  def getSourceToFile(sf: SharedFile)(
      implicit service: FileServiceComponent,
      context: ActorRefFactory,
      ec: ExecutionContext): Source[ByteString, _] =
    sf.path match {
      case path: RemoteFilePath => service.remote.createSource(path)
      case path: ManagedFilePath =>
        if (service.storage.isDefined) {
          service.storage.get.createSource(path)
        } else {
          val serviceactor = service.actor
          implicit val timout = akka.util.Timeout(1441 minutes)
          val ac = context.actorOf(
            Props(
              new FileUserSource(path,
                                 sf.byteSize,
                                 sf.hash,
                                 serviceactor,
                                 isLocal))
              .withDispatcher("fileuser-dispatcher"))

          val f = (ac ? WaitingForPath)
            .asInstanceOf[Future[Try[Source[ByteString, _]]]]

          f onComplete {
            case _ => ac ! PoisonPill
          }

          val f2 = f map (_ match {
            case Success(r) => r
            case Failure(e) =>
              throw new RuntimeException("getSourceToFile failed. " + sf, e)
          })

          Source.fromFuture(f2).flatMapConcat(x => x)
        }
    }

  def getPathToFile(sf: SharedFile)(implicit service: FileServiceComponent,
                                    context: ActorRefFactory,
                                    nlc: NodeLocalCacheActor,
                                    ec: ExecutionContext): Future[File] =
    NodeLocalCache._getItemAsync("fs::" + sf) {
      sf.path match {
        case p: RemoteFilePath => service.remote.exportFile(p)
        case p: ManagedFilePath =>
          if (service.storage.isDefined) {
            service.storage.get.exportFile(p)
          } else {
            val serviceactor = service.actor
            implicit val timout = akka.util.Timeout(1441 minutes)
            val ac = context.actorOf(
              Props(
                new FileUser(p, sf.byteSize, sf.hash, serviceactor, isLocal))
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

  def createFromFile(file: File, name: String, deleteFile: Boolean)(
      implicit prefix: FileServicePrefix,
      ec: ExecutionContext,
      service: FileServiceComponent,
      context: ActorRefFactory,
      config: TasksConfig) =
    if (service.storage.isDefined) {
      val proposedPath = prefix.propose(name)
      service.storage.get.importFile(file, proposedPath).map { f =>
        if (deleteFile) {
          file.delete
        }
        SharedFileHelper.create(f._1, f._2, f._4, proposedPath.history)
      }
    } else {
      val serviceactor = service.actor
      if (!file.canRead) {
        throw new java.io.FileNotFoundException("not found" + file)
      }

      implicit val timout = akka.util.Timeout(1441 minutes)

      val ac = context.actorOf(
        Props(
          new FileSender(file, prefix.propose(name), deleteFile, serviceactor))
          .withDispatcher("filesender-dispatcher"))
      (ac ? WaitingForSharedFile)
        .asInstanceOf[Future[Option[SharedFile]]]
        .map(_.get)
        .andThen { case _ => ac ! PoisonPill }

    }

  def createFromSource(source: Source[ByteString, _], name: String)(
      implicit prefix: FileServicePrefix,
      ec: ExecutionContext,
      service: FileServiceComponent,
      context: ActorRefFactory,
      mat: Materializer,
      config: TasksConfig) =
    if (service.storage.isDefined) {
      val proposedPath = prefix.propose(name)
      service.storage.get.importSource(source, proposedPath).map { x =>
        SharedFileHelper.create(x._1, x._2, x._3, proposedPath.history)
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

}
