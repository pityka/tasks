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

import java.io.File
import tasks.util.Uri
import tasks.TaskSystemComponents
import tasks.Implicits._
import tasks.HasSharedFiles
import tasks.fs
import tasks.tasksConfig

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import cats.effect.kernel.Resource
import fs2.Stream
import fs2.Pipe

sealed trait FilePath {
  def name: String
}

case class ManagedFilePath(pathElements: Vector[String]) extends FilePath {
  override def toString = "/" + pathElements.mkString("/")
  def name = pathElements.last
}

case class RemoteFilePath(uri: Uri) extends FilePath {
  override def toString = uri.toString
  def name = uri.path.toString.split("/").filter(_.size > 0).last
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

  def file(implicit tsc: TaskSystemComponents): Resource[IO, File] =
    SharedFileHelper.getPathToFile(this)

  def fileNonCached(implicit tsc: TaskSystemComponents): Resource[IO, File] =
    SharedFileHelper.getPathToFileNonCachedFile(this)

  def history(implicit tsc: TaskSystemComponents): IO[History] =
    SharedFileHelper.getHistory(this)

  def stream(implicit tsc: TaskSystemComponents): Stream[IO, Byte] =
    SharedFileHelper.stream(this, 0L)

  def stream(fromOffset: Long)(implicit
      tsc: TaskSystemComponents
  ): Stream[IO, Byte] =
    SharedFileHelper.stream(this, fromOffset)

  def isAccessible(implicit tsc: TaskSystemComponents): IO[Boolean] =
    SharedFileHelper.isAccessible(this, true)

  def uri(implicit tsc: TaskSystemComponents): IO[Uri] =
    SharedFileHelper.getUri(this)

  def name: String = path.name

  def delete(implicit tsc: TaskSystemComponents): IO[Boolean] =
    SharedFileHelper.delete(this)

  def utf8(implicit tsc: TaskSystemComponents): IO[String] =
    stream(0L).through(fs2.text.utf8.decode).compile.lastOrError
  def bytes(implicit tsc: TaskSystemComponents): IO[scodec.bits.ByteVector] =
    stream.compile.to(scodec.bits.ByteVector)
}

object SharedFile {

  implicit val codec: JsonValueCodec[SharedFile] = JsonCodecMaker.make

  def apply(uri: Uri)(implicit tsc: TaskSystemComponents): IO[SharedFile] =
    SharedFileHelper.create(RemoteFilePath(uri), tsc.fs.remote)

  def apply(file: File, name: String)(implicit
      tsc: TaskSystemComponents
  ): IO[SharedFile] =
    apply(file, name, false)

  def apply(file: File, name: String, deleteFile: Boolean)(implicit
      tsc: TaskSystemComponents
  ): IO[SharedFile] =
    SharedFileHelper.createFromFile(file, name, deleteFile)

  def apply(source: Stream[IO, Byte], name: String)(implicit
      tsc: TaskSystemComponents
  ): IO[SharedFile] =
    SharedFileHelper.createFromStream(source, name)

  def apply(bytes: Array[Byte], name: String)(implicit
      tsc: TaskSystemComponents
  ): IO[SharedFile] =
    this.apply(fs2.Stream.chunk(fs2.Chunk.array(bytes)), name)

  def sink(name: String)(implicit
      tsc: TaskSystemComponents
  ): Pipe[IO, Byte, SharedFile] =
    SharedFileHelper.sink(name)

  def fromFolder(parallelism: Int)(
      callback: File => List[File]
  )(implicit tsc: TaskSystemComponents): IO[Seq[SharedFile]] =
    SharedFileHelper.createFromFolder(parallelism)(callback)

  def getByName(
      name: String
  )(implicit tsc: TaskSystemComponents): IO[Option[SharedFile]] =
    SharedFileHelper.getByName(name, retrieveSizeAndHash = true)

}

object FilePath {
  implicit val codec: JsonValueCodec[FilePath] = JsonCodecMaker.make
}
