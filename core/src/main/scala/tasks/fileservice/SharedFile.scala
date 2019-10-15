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

import akka.stream.scaladsl._
import akka.util._
import scala.concurrent.Future
import java.io.File
import tasks.util._
import scala.concurrent._
import tasks.util.Uri
import tasks.TaskSystemComponents
import tasks.Implicits._
import tasks.HasSharedFiles

import io.circe.{Decoder, Encoder, DecodingFailure}
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
) extends HasSharedFiles {

  private[tasks] def mutableFiles = Nil

  private[tasks] def immutableFiles = List(this)

  override def toString =
    s"SharedFile($path, size=$byteSize, hash=$hash)"

  def file(implicit tsc: TaskSystemComponents) =
    SharedFileHelper.getPathToFile(this)

  def history(implicit tsc: TaskSystemComponents): Future[History] =
    SharedFileHelper.getHistory(this)

  def source(implicit tsc: TaskSystemComponents) =
    SharedFileHelper.getSourceToFile(this, 0L)

  def source(fromOffset: Long)(implicit tsc: TaskSystemComponents) =
    SharedFileHelper.getSourceToFile(this, fromOffset)

  def isAccessible(implicit tsc: TaskSystemComponents) =
    SharedFileHelper.isAccessible(this, true)

  def uri(implicit tsc: TaskSystemComponents): Future[Uri] =
    SharedFileHelper.getUri(this)

  def name = path.name

  def delete(implicit tsc: TaskSystemComponents): Future[Boolean] =
    SharedFileHelper.delete(this)
}

object SharedFile {

  implicit val encoder: Encoder[SharedFile] =
    Encoder.forProduct3("path", "byteSize", "hash")(
      sf => (sf.path, sf.byteSize, sf.hash)
    )

  implicit val decoder: Decoder[SharedFile] =
    Decoder.instance { cursor =>
      cursor.focus.flatMap(_.asObject) match {
        case None => Left(DecodingFailure("not object", Nil))
        case Some(_) =>
          Decoder
            .forProduct3("path", "byteSize", "hash")(
              (a: FilePath, b: Long, c: Int) => new SharedFile(a, b, c)
            )
            .apply(cursor)

      }
    }

  def apply(uri: Uri)(implicit tsc: TaskSystemComponents): Future[SharedFile] =
    SharedFileHelper.create(RemoteFilePath(uri), tsc.fs.remote)

  def apply(file: File, name: String)(
      implicit tsc: TaskSystemComponents
  ): Future[SharedFile] =
    apply(file, name, false)

  def apply(file: File, name: String, deleteFile: Boolean)(
      implicit tsc: TaskSystemComponents
  ): Future[SharedFile] =
    SharedFileHelper.createFromFile(file, name, deleteFile)

  def apply(source: Source[ByteString, _], name: String)(
      implicit tsc: TaskSystemComponents
  ): Future[SharedFile] =
    SharedFileHelper.createFromSource(source, name)

  def sink(name: String)(
      implicit tsc: TaskSystemComponents
  ): Option[Sink[ByteString, Future[SharedFile]]] =
    SharedFileHelper.sink(name)

  def fromFolder(
      callback: File => List[File]
  )(implicit tsc: TaskSystemComponents): Future[Seq[SharedFile]] =
    SharedFileHelper.createFromFolder(callback)

  def getByName(
      name: String
  )(implicit tsc: TaskSystemComponents): Future[Option[SharedFile]] =
    SharedFileHelper.getByName(name, retrieveSizeAndHash = true)

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
