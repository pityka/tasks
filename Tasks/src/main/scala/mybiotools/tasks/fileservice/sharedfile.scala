/*
* The MIT License
*
* Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
* Group Fellay
* Copyright (c) 2016 Istvan Bartha
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

import akka.actor.{ Actor, PoisonPill, ActorRef, Props, ActorRefFactory }
import akka.actor.Actor._
import akka.pattern.ask
import akka.pattern.pipe
import scala.concurrent.{ Future, Await, ExecutionContext }
import java.lang.Class
import java.io.{ File, InputStream, FileInputStream, BufferedInputStream, OutputStream }
import scala.concurrent.duration._
import java.util.concurrent.{ TimeUnit, ScheduledFuture }
import java.nio.channels.{ WritableByteChannel, ReadableByteChannel }
import tasks.util._
import scala.util.{ Try, Failure, Success }
import com.google.common.hash._
import scala.concurrent._
import scala.util._
import java.net.URL

import tasks.util.eq._

sealed trait FilePath {
  def name: String
}

@SerialVersionUID(1L)
case class ManagedFilePath(pathElements: Vector[String]) extends FilePath {
  override def toString = "/" + pathElements.mkString("/")
  def name = pathElements.last
}

@SerialVersionUID(1L)
case class RemoteFilePath(url: URL) extends FilePath {
  override def toString = url.toString
  def name = url.getFile
}

@SerialVersionUID(1L)
class SharedFile private[tasks] (
    val path: FilePath,
    val byteSize: Long,
    val hash: Int
) extends Serializable with Serializable {
  def canEqual(other: Any): Boolean = other.isInstanceOf[SharedFile]

  override def hashCode: Int = 41 * (41 * (41 * (path.hashCode + 41) + byteSize.hashCode) + hash.hashCode)

  override def equals(that: Any): Boolean = that match {
    case t: SharedFile => t.canEqual(this) && t.path === this.path && t.byteSize === this.byteSize && t.hash === this.hash
    case _ => false
  }

  override def toString = s"SharedFile($path)"

  def file(implicit service: FileServiceActor, context: ActorRefFactory) =
    SharedFileHelper.getPathToFile(this)

  def openStream[R](f: InputStream => R)(implicit service: FileServiceActor, context: ActorRefFactory) =
    SharedFileHelper.openStreamToFile(this)(f)

  def isAccessible(implicit service: FileServiceActor, context: ActorRefFactory) =
    SharedFileHelper.isAccessible(this)

  def url(implicit service: FileServiceActor, context: ActorRefFactory): URL = SharedFileHelper.getURL(this)

  def name = path.name
}

object SharedFile {

  // def openOutputStream[R](name: String)(f: OutputStream => R)(implicit service: FileServiceActor, context: ActorRefFactory): (R, SharedFile)

  def apply(url: URL)(implicit service: FileServiceActor, context: ActorRefFactory, prefix: FileServicePrefix): SharedFile = {

    val serviceactor = service.actor
    rethrow("Can't open URL: " + url) {
      val is = url.openStream
      is.read
      is.close
    }

    implicit val timout = akka.util.Timeout(1441 minutes)

    Await.result((serviceactor ? NewRemote(url)).asInstanceOf[Future[Try[SharedFile]]], atMost = 1440 minutes).get

  }

  def apply(file: File, name: String)(implicit service: FileServiceActor, context: ActorRefFactory, prefix: FileServicePrefix): SharedFile = {

    val serviceactor = service.actor
    if (!file.canRead) {
      throw new java.io.FileNotFoundException("not found" + file)
    }

    implicit val timout = akka.util.Timeout(1441 minutes)

    val ac = context.actorOf(Props(new FileSender(file, prefix.propose(name), serviceactor)).withDispatcher("filesender-dispatcher"))
    val f = Await.result((ac ? WaitingForSharedFile).asInstanceOf[Future[Option[SharedFile]]], atMost = 1440 minutes).get
    ac ! PoisonPill
    f
  }

  def apply(file: File)(implicit service: FileServiceActor, context: ActorRefFactory, prefix: FileServicePrefix): SharedFile = apply(file, if (tasks.util.config.includeFullPathInDefaultSharedName) file.getAbsolutePath.replace(File.separator, "_") else file.getName)

}
