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
import tasks.ExtendedTaskSystemComponents
import tasks.TaskSystemComponents
import tasks.Implicits._
import tasks.HasSharedFiles
import tasks.fs
import tasks.tasksConfig
import tasks.queue.ComputationEnvironment

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import cats.effect.kernel.Resource
import fs2.Stream
import fs2.Pipe

sealed trait FilePath {
  def name: String
}

object FilePath {
  private[fileservice] object view {
    sealed trait V
    final case class ManagedFilePath(pathElements: Vector[String]) extends V
    final case class RemoteFilePath(uri: Uri) extends V
  }

  private[fileservice] def toView(p: FilePath): view.V = p match {
    case ManagedFilePath(elements) => view.ManagedFilePath(elements)
    case RemoteFilePath(uri)       => view.RemoteFilePath(uri)
  }

  private[fileservice] def fromView(v: view.V): FilePath = v match {
    case view.ManagedFilePath(elements) => ManagedFilePath(elements)
    case view.RemoteFilePath(uri)       => RemoteFilePath(uri)
  }

  implicit val codec: JsonValueCodec[FilePath] = {
    val innerCodec: JsonValueCodec[view.V] = JsonCodecMaker.make
    new JsonValueCodec[FilePath] {
      val nullValue: FilePath = null
      def encodeValue(x: FilePath, out: JsonWriter): Unit =
        innerCodec.encodeValue(toView(x), out)
      def decodeValue(in: JsonReader, default: FilePath): FilePath =
        fromView(innerCodec.decodeValue(in, null))
    }
  }
}

sealed trait ManagedFilePath extends FilePath {
  def pathElements: Vector[String]
}
object ManagedFilePath {
  private[tasks] final case class ManagedFilePathImpl(
      pathElements: Vector[String]
  ) extends ManagedFilePath {
    override def toString = "/" + pathElements.mkString("/")
    def name = pathElements.last
  }
  def apply(pathElements: Vector[String]): ManagedFilePath =
    ManagedFilePathImpl(pathElements)
  def unapply(m: ManagedFilePath): Some[Vector[String]] = Some(m.pathElements)
}

sealed trait RemoteFilePath extends FilePath {
  def uri: Uri
}
object RemoteFilePath {
  private[tasks] final case class RemoteFilePathImpl(uri: Uri)
      extends RemoteFilePath {
    override def toString = uri.toString
    def name = uri.path.toString.split("/").filter(_.size > 0).last
  }
  def apply(uri: Uri): RemoteFilePath = RemoteFilePathImpl(uri)
  def unapply(r: RemoteFilePath): Some[Uri] = Some(r.uri)
  implicit def toLogFeature(rm: RemoteFilePath): scribe.LogFeature =
    scribe.data(
      Map(
        "remote-file-path-uri" -> rm.uri
      )
    )
}

sealed trait SharedFile extends HasSharedFiles {
  def path: FilePath
  def byteSize: Long
  def hash: Int

  private[tasks] final def mutableFiles = Nil

  private[tasks] final def immutableFiles = List(this)

  override def toString =
    s"SharedFile($path, size=$byteSize, hash=$hash)"

  final def file(implicit tsc: TaskSystemComponents): Resource[IO, File] =
    SharedFileHelper.getPathToFile(this)

  final def filePersistent(implicit
      tsc: TaskSystemComponents
  ): Resource[IO, File] =
    SharedFileHelper.getPathToFile(this, keepLocalCache = true)

  final def fileNonCached(implicit
      tsc: TaskSystemComponents
  ): Resource[IO, File] =
    SharedFileHelper.getPathToFileNonCachedFile(this)

  final def history(implicit tsc: TaskSystemComponents): IO[History] =
    SharedFileHelper.getHistory(this)

  final def stream(implicit tsc: TaskSystemComponents): Stream[IO, Byte] =
    SharedFileHelper.stream(this, 0L)

  final def stream(fromOffset: Long)(implicit
      tsc: TaskSystemComponents
  ): Stream[IO, Byte] =
    SharedFileHelper.stream(this, fromOffset)

  final def isAccessible(implicit tsc: TaskSystemComponents): IO[Boolean] =
    SharedFileHelper.isAccessible(this, true)

  final def uri(implicit tsc: TaskSystemComponents): IO[Uri] =
    SharedFileHelper.getUri(this)

  final def name: String = path.name

  final def delete(implicit tsc: TaskSystemComponents): IO[Boolean] =
    SharedFileHelper.delete(this)

  final def utf8(implicit tsc: TaskSystemComponents): IO[String] =
    stream(0L).through(fs2.text.utf8.decode).compile.string
  final def bytes(implicit
      tsc: TaskSystemComponents
  ): IO[scodec.bits.ByteVector] =
    stream.compile.to(scodec.bits.ByteVector)
}

object SharedFile {

  private[tasks] final case class SharedFileImpl(
      path: FilePath,
      byteSize: Long,
      hash: Int
  ) extends SharedFile

  def apply(path: FilePath, byteSize: Long, hash: Int): SharedFile =
    SharedFileImpl(path, byteSize, hash)

  implicit val codec: JsonValueCodec[SharedFile] = {
    val innerCodec: JsonValueCodec[SharedFileImpl] = JsonCodecMaker.make
    new JsonValueCodec[SharedFile] {
      val nullValue: SharedFile = null
      def encodeValue(
          x: SharedFile,
          out: com.github.plokhotnyuk.jsoniter_scala.core.JsonWriter
      ): Unit =
        innerCodec.encodeValue(x.asInstanceOf[SharedFileImpl], out)
      def decodeValue(
          in: com.github.plokhotnyuk.jsoniter_scala.core.JsonReader,
          default: SharedFile
      ): SharedFile =
        innerCodec.decodeValue(in, null)
    }
  }

  def apply(uri: Uri)(implicit tsc: TaskSystemComponents): IO[SharedFile] =
    SharedFileHelper.create(RemoteFilePath(uri), tsc.fs.remote)

  def fromFolder(parallelism: Int)(
      callback: File => List[File]
  )(implicit ce: ComputationEnvironment): IO[Seq[SharedFile]] = {
    val tsc = ce.toExtendedTaskSystemComponents
    SharedFileHelper.createFromFolder(parallelism)(callback)(
      tsc.filePrefix,
      tsc.fs,
      tsc.tasksConfig,
      tsc.historyContext
    )
  }

  def getByName(
      name: String
  )(implicit tsc: TaskSystemComponents): IO[Option[SharedFile]] =
    SharedFileHelper.getByName(name, retrieveSizeAndHash = true)

  def scoped(file: File, suffix: String)(implicit
      ce: ComputationEnvironment
  ): IO[SharedFile] =
    scoped(file, suffix, deleteFile = false)

  def scoped(file: File, suffix: String, deleteFile: Boolean)(implicit
      ce: ComputationEnvironment
  ): IO[SharedFile] = {
    val tsc = ce.toExtendedTaskSystemComponents
    SharedFileHelper.createFromFile(file, suffix, deleteFile)(
      tsc.filePrefix.append(ce.prefix.value),
      tsc.fs,
      tsc.tasksConfig,
      tsc.historyContext
    )
  }

  def scoped(source: Stream[IO, Byte], suffix: String)(implicit
      ce: ComputationEnvironment
  ): IO[SharedFile] = {
    val tsc = ce.toExtendedTaskSystemComponents
    SharedFileHelper.createFromStream(source, suffix)(
      tsc.filePrefix.append(ce.prefix.value),
      tsc.fs,
      tsc.tasksConfig,
      tsc.historyContext
    )
  }

  def scoped(bytes: Array[Byte], suffix: String)(implicit
      ce: ComputationEnvironment
  ): IO[SharedFile] =
    scoped(fs2.Stream.chunk(fs2.Chunk.array(bytes)), suffix)

  def scopedSink(suffix: String)(implicit
      ce: ComputationEnvironment
  ): Pipe[IO, Byte, SharedFile] = {
    val tsc = ce.toExtendedTaskSystemComponents
    SharedFileHelper.sink(suffix)(
      tsc.filePrefix.append(ce.prefix.value),
      tsc.fs,
      tsc.tasksConfig,
      tsc.historyContext
    )
  }

  def unscoped(file: File, suffix: String)(implicit
      ce: ComputationEnvironment
  ): IO[SharedFile] =
    unscoped(file, suffix, deleteFile = false)

  def unscoped(file: File, suffix: String, deleteFile: Boolean)(implicit
      ce: ComputationEnvironment
  ): IO[SharedFile] = {
    val tsc = ce.toExtendedTaskSystemComponents
    SharedFileHelper.createFromFile(file, suffix, deleteFile)(
      tsc.filePrefix,
      tsc.fs,
      tsc.tasksConfig,
      tsc.historyContext
    )
  }

  def unscoped(source: Stream[IO, Byte], suffix: String)(implicit
      ce: ComputationEnvironment
  ): IO[SharedFile] = {
    val tsc = ce.toExtendedTaskSystemComponents
    SharedFileHelper.createFromStream(source, suffix)(
      tsc.filePrefix,
      tsc.fs,
      tsc.tasksConfig,
      tsc.historyContext
    )
  }

  def unscoped(bytes: Array[Byte], suffix: String)(implicit
      ce: ComputationEnvironment
  ): IO[SharedFile] =
    unscoped(fs2.Stream.chunk(fs2.Chunk.array(bytes)), suffix)

  def unscopedSink(suffix: String)(implicit
      ce: ComputationEnvironment
  ): Pipe[IO, Byte, SharedFile] = {
    val tsc = ce.toExtendedTaskSystemComponents
    SharedFileHelper.sink(suffix)(
      tsc.filePrefix,
      tsc.fs,
      tsc.tasksConfig,
      tsc.historyContext
    )
  }

}
