package tasks

import tasks.queue._
import java.time.Instant
import tasks.util.Uri

package object upicklesupport {
  import upickle.default._
  implicit def ser[A](implicit enc: Writer[A]): Serializer[A] =
    new Serializer[A] {
      def apply(a: A) = upickle.default.write(a).getBytes("UTF-8")
    }
  implicit def deser[A](implicit dec: Reader[A]): Deserializer[A] =
    new Deserializer[A] {

      def apply(b: Array[Byte]) =
        scala.util
          .Try(upickle.default.read[A](new String(b)))
          .toEither
          .left
          .map(_.toString)
    }

  implicit val instantRW: ReadWriter[Instant] =
    upickle.default
      .readwriter[Long]
      .bimap[java.time.Instant](
        instant => instant.toEpochMilli,
        { case num =>
          java.time.Instant.ofEpochMilli(num)
        }
      )

  implicit val uri: ReadWriter[Uri] = {
    val implRW: ReadWriter[tasks.util.Uri.UriImpl] =
      upickle.default.macroRW[tasks.util.Uri.UriImpl]
    implRW.bimap[Uri](
      u => u.asInstanceOf[tasks.util.Uri.UriImpl],
      impl => impl
    )
  }

  implicit val rpath: ReadWriter[tasks.fileservice.RemoteFilePath] = {
    val implRW
        : ReadWriter[tasks.fileservice.RemoteFilePath.RemoteFilePathImpl] =
      upickle.default
        .macroRW[tasks.fileservice.RemoteFilePath.RemoteFilePathImpl]
    implRW.bimap[tasks.fileservice.RemoteFilePath](
      r => r.asInstanceOf[tasks.fileservice.RemoteFilePath.RemoteFilePathImpl],
      impl => impl
    )
  }

  implicit val mpath: ReadWriter[tasks.fileservice.ManagedFilePath] = {
    val implRW
        : ReadWriter[tasks.fileservice.ManagedFilePath.ManagedFilePathImpl] =
      upickle.default
        .macroRW[tasks.fileservice.ManagedFilePath.ManagedFilePathImpl]
    implRW.bimap[tasks.fileservice.ManagedFilePath](
      m =>
        m.asInstanceOf[tasks.fileservice.ManagedFilePath.ManagedFilePathImpl],
      impl => impl
    )
  }

  implicit val filepath: ReadWriter[tasks.fileservice.FilePath] =
    upickle.default.macroRW[tasks.fileservice.FilePath]

  implicit val sharedFileRW: ReadWriter[tasks.fileservice.SharedFile] = {
    val implRW: ReadWriter[tasks.fileservice.SharedFile.SharedFileImpl] =
      upickle.default.macroRW[tasks.fileservice.SharedFile.SharedFileImpl]
    implRW.bimap[tasks.fileservice.SharedFile](
      sf => sf.asInstanceOf[tasks.fileservice.SharedFile.SharedFileImpl],
      impl => impl
    )
  }

}
