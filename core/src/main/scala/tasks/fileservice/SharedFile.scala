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
import akka.stream._
import akka.util._
import scala.concurrent.{Future, ExecutionContext}
import java.lang.Class
import java.io.{
  File,
  InputStream,
  FileInputStream,
  BufferedInputStream,
  OutputStream
}
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

import io.circe.{Json, Decoder, Encoder}
import io.circe.syntax._
import io.circe.generic.semiauto._

sealed trait FilePath {
  def name: String
}

case class ManagedFilePath(pathElements: Vector[String]) extends FilePath {
  override def toString = "/" + pathElements.mkString("/")
  def name = pathElements.last
}

case class RemoteFilePath(uri: Uri) extends FilePath {
  override def toString = uri.toString
  def name = uri.akka.path.toString.split("/").filter(_.size > 0).last
}

case class SharedFile(
    path: FilePath,
    byteSize: Long,
    hash: Int
) {  

  override def toString = s"SharedFile($path)"

  def file(implicit tsc: TaskSystemComponents) =
    SharedFileHelper.getPathToFile(this)

  def source(implicit tsc: TaskSystemComponents) =
    SharedFileHelper.getSourceToFile(this)

  def isAccessible(implicit tsc: TaskSystemComponents) =
    SharedFileHelper.isAccessible(this)

  def uri(implicit tsc: TaskSystemComponents): Future[Uri] =
    SharedFileHelper.getUri(this)

  def name = path.name
}

object SharedFile {

  implicit val encoder: Encoder[SharedFile] = //deriveEncoder[SharedFile]
    Encoder.forProduct3("path", "byteSize", "hash")(sf =>
      (sf.path, sf.byteSize, sf.hash))

  implicit val decoder: Decoder[SharedFile] = //deriveDecoder[SharedFile]
    Decoder.forProduct3("path", "byteSize", "hash")(
      (a: FilePath, b: Long, c: Int) => new SharedFile(a, b, c))

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

object ManagedFilePath {
  implicit val decoder: Decoder[ManagedFilePath] =
    deriveDecoder[ManagedFilePath]
  implicit val encoder: Encoder[ManagedFilePath] =
    deriveEncoder[ManagedFilePath]
}

object RemoteFilePath {
  implicit val decoder: Decoder[RemoteFilePath] = deriveDecoder[RemoteFilePath]
  implicit val encoder: Encoder[RemoteFilePath] = deriveEncoder[RemoteFilePath]
}

object FilePath {
  implicit val decoder: Decoder[FilePath] = deriveDecoder[FilePath]
  implicit val encoder: Encoder[FilePath] = deriveEncoder[FilePath]
}
