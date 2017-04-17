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
import akka.stream.scaladsl._
import akka.stream.ActorMaterializer
import akka.util._
import scala.concurrent.{Future, ExecutionContext}
import java.lang.Class
import java.io.{File, InputStream, FileInputStream, BufferedInputStream, OutputStream}
import scala.concurrent.duration._
import java.util.concurrent.{TimeUnit, ScheduledFuture}
import java.nio.channels.{WritableByteChannel, ReadableByteChannel}
import tasks.util._
import scala.util.{Try, Failure, Success}
import com.google.common.hash._
import scala.concurrent._
import scala.util._

import tasks.util.eq._
import tasks.util.Uri
import tasks.queue._
import tasks.TaskSystemComponents
import tasks.Implicits._

import upickle.default._
import upickle.Js

sealed trait FilePath {
  def name: String
}

@SerialVersionUID(1L)
case class ManagedFilePath(pathElements: Vector[String]) extends FilePath {
  override def toString = "/" + pathElements.mkString("/")
  def name = pathElements.last
}

@SerialVersionUID(1L)
case class RemoteFilePath(uri: Uri) extends FilePath {
  override def toString = uri.toString
  def name = uri.akka.path.toString.split("/").filter(_.size > 0).last
}

@SerialVersionUID(1L)
class SharedFile private[tasks] (
    val path: FilePath,
    val byteSize: Long,
    val hash: Int
) extends Serializable
    with Serializable {
  def canEqual(other: Any): Boolean = other.isInstanceOf[SharedFile]

  override def hashCode: Int =
    41 * (41 * (41 * (path.hashCode + 41) + byteSize.hashCode) + hash.hashCode)

  override def equals(that: Any): Boolean = that match {
    case t: SharedFile =>
      t.canEqual(this) && t.path === this.path && t.byteSize === this.byteSize && t.hash === this.hash
    case _ => false
  }

  override def toString = s"SharedFile($path)"

  def file(implicit tsc: TaskSystemComponents) =
    SharedFileHelper.getPathToFile(this)

  def source(implicit tsc: TaskSystemComponents) =
    SharedFileHelper.getSourceToFile(this)

  def openStream[R](f: InputStream => R)(implicit tsc: TaskSystemComponents) =
    SharedFileHelper.openStreamToFile(this)(f)

  def isAccessible(implicit tsc: TaskSystemComponents) =
    SharedFileHelper.isAccessible(this)

  def uri(implicit tsc: TaskSystemComponents): Future[Uri] =
    SharedFileHelper.getUri(this)

  def name = path.name
}

object SharedFile {

  implicit val fpWriter = Writer[FilePath] {
    case ManagedFilePath(pathElements) =>
      Js.Obj("type" -> Js.Str("managed"),
             "path" -> Js.Arr(pathElements.map(x => Js.Str(x)): _*))
    case RemoteFilePath(uri) =>
      Js.Obj("type" -> Js.Str("remote"), "path" -> Js.Str(uri.toString))
  }

  implicit val fpReader = Reader[FilePath] {
    case Js.Obj(p @ _ *) =>
      val map = p.toMap
      map("type").str match {
        case "managed" => ManagedFilePath(map("path").arr.toVector.map(_.str))
        case "remote" => RemoteFilePath(Uri(map("path").str))
      }
  }

  implicit val sfWriter = Writer[SharedFile] {
    case t =>
      Js.Obj("path" -> writeJs(t.path),
             "byteSize" -> Js.Str(t.byteSize.toString),
             "hash" -> Js.Str(t.hash.toString))
  }

  implicit val sfReader = Reader[SharedFile] {
    case Js.Obj(p @ _ *) =>
      val map = p.toMap
      new SharedFile(fpReader.read(map("path")),
                     map("byteSize").str.toLong,
                     map("hash").str.toInt)
  }

  def apply(uri: Uri)(implicit tsc: TaskSystemComponents): Future[SharedFile] =
    SharedFileHelper.create(RemoteFilePath(uri), tsc.fs.remote)

  def apply(file: File, name: String)(
      implicit tsc: TaskSystemComponents): Future[SharedFile] =
    apply(file, name, false)

  def apply(file: File, name: String, deleteFile: Boolean)(
      implicit tsc: TaskSystemComponents): Future[SharedFile] =
    SharedFileHelper.createFromFile(file, name, deleteFile)

  def apply(source: Source[ByteString, _], name: String)(
      implicit tsc: TaskSystemComponents): Future[SharedFile] =
    SharedFileHelper.createFromSource(source, name)

}
