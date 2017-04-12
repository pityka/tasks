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
import akka.pattern.pipe
import akka.stream.scaladsl._
import akka.stream._
import akka.util._

import com.google.common.hash._

import scala.concurrent.duration._
import scala.concurrent._
import scala.util._

import java.lang.Class
import java.io.{File, InputStream, FileInputStream, BufferedInputStream}
import java.util.concurrent.{TimeUnit, ScheduledFuture}
import java.nio.channels.{WritableByteChannel, ReadableByteChannel}

import tasks.util._
import tasks.util.eq._
import tasks.caching._
import tasks.queue._

object SharedFileHelper {

  private[tasks] def getByNameUnchecked(name: String)(
      implicit service: FileServiceActor,
      context: ActorRefFactory,
      nlc: NodeLocalCacheActor,
      prefix: FileServicePrefix,
      ec: ExecutionContext): Future[SharedFile] = {
    getPathToFile(new SharedFile(prefix.propose(name).toManaged, -1L, 0)).map {
      f =>
        new SharedFile(prefix.propose(name).toManaged, -1L, 0)
    }

  }

  private[tasks] def createForTesting(name: String) =
    new SharedFile(ManagedFilePath(Vector(name)), 0, 0)
  private[tasks] def createForTesting(name: String, size: Long, hash: Int) =
    new SharedFile(ManagedFilePath(Vector(name)), size, hash)

  private[tasks] def create(size: Long,
                            hash: Int,
                            path: ManagedFilePath): SharedFile =
    new SharedFile(path, size, hash)

  private[tasks] def create(path: RemoteFilePath, storage: RemoteFileStorage)(
      implicit ec: ExecutionContext): Future[SharedFile] =
    storage.getSizeAndHash(path).map {
      case (size, hash) =>
        new SharedFile(path, size, hash)
    }

  private val isLocal = (f: File) => f.canRead

  def getStreamToFile(sf: SharedFile)(
      implicit service: FileServiceActor,
      context: ActorRefFactory,
      ec: ExecutionContext): Future[InputStream] = sf.path match {
    case path: RemoteFilePath =>
      Future.fromTry(service.remote.createStream(path))
    case path: ManagedFilePath => {
      if (service.storage.isDefined) {
        Future.fromTry(service.storage.get.createStream(path))
      } else {
        val serviceactor = service.actor
        implicit val timout = akka.util.Timeout(1441 minutes)
        val ac = context.actorOf(
            Props(
                new FileUserStream(path,
                                   sf.byteSize,
                                   sf.hash,
                                   serviceactor,
                                   isLocal))
              .withDispatcher("fileuser-dispatcher"))

        val f = (ac ? WaitingForPath).asInstanceOf[Future[Try[InputStream]]]

        f onComplete {
          case _ => ac ! PoisonPill
        }

        f map (_ match {
              case Success(r) => r
              case Failure(e) =>
                throw new RuntimeException("getStreamToFile failed. " + sf, e)
            })
      }
    }
  }

  def getSourceToFile(sf: SharedFile)(
      implicit service: FileServiceActor,
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
                  throw new RuntimeException("getSourceToFile failed. " + sf,
                                             e)
              })

          Source.fromFuture(f2).flatMapConcat(x => x)
        }
    }

  def openStreamToFile[R](sf: SharedFile)(fun: InputStream => R)(
      implicit service: FileServiceActor,
      context: ActorRefFactory,
      ec: ExecutionContext): Future[R] = getStreamToFile(sf).map { is =>
    val r = fun(is)
    is.close
    r
  }

  def getPathToFile(sf: SharedFile)(implicit service: FileServiceActor,
                                    context: ActorRefFactory,
                                    nlc: NodeLocalCacheActor,
                                    ec: ExecutionContext): Future[File] =
    NodeLocalCache.getItemAsync("fs::" + sf) {
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
                    new FileUser(p,
                                 sf.byteSize,
                                 sf.hash,
                                 serviceactor,
                                 isLocal))
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

  def isAccessible(sf: SharedFile)(implicit service: FileServiceActor,
                                   context: ActorRefFactory): Future[Boolean] =
    sf.path match {
      case x: RemoteFilePath =>
        service.remote.contains(x, sf.byteSize, sf.hash)
      case path: ManagedFilePath =>
        if (service.storage.isDefined) {
          Future.successful(
              service.storage.get.contains(path, sf.byteSize, sf.hash))
        } else {
          implicit val timout = akka.util.Timeout(1441 minutes)

          (service.actor ? IsAccessible(path, sf.byteSize, sf.hash))
            .asInstanceOf[Future[Boolean]]
        }

    }

  def getUri(sf: SharedFile)(implicit service: FileServiceActor,
                             context: ActorRefFactory): Future[Uri] =
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

}
