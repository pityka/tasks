package tasks

import tasks.queue._
import com.google.common.{hash => ghash}
import com.google.common.io.ByteStreams
import java.time.Instant
import tasks.util.Uri

package object upicklesupport {
  import upickle.default._
  implicit def ser[A](implicit enc: Writer[A]): Serializer[A] =
    new Serializer[A] {

      override def hash(a: A): String = {
        val os = new ghash.HashingOutputStream(
          ghash.Hashing.murmur3_128,
          ByteStreams.nullOutputStream()
        )
        upickle.default.writeToOutputStream(a, os)
        os.hash().toString()
      }

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

  implicit val uri: ReadWriter[Uri] =
    upickle.default.macroRW[tasks.util.Uri]

  implicit val rpath: ReadWriter[tasks.fileservice.RemoteFilePath] =
    upickle.default.macroRW[tasks.fileservice.RemoteFilePath]

  implicit val mpath: ReadWriter[tasks.fileservice.ManagedFilePath] =
    upickle.default.macroRW[tasks.fileservice.ManagedFilePath]

  implicit val filepath: ReadWriter[tasks.fileservice.FilePath] =
    upickle.default.macroRW[tasks.fileservice.FilePath]

  implicit val sharedFileRW: ReadWriter[tasks.fileservice.SharedFile] =
    upickle.default.macroRW[tasks.fileservice.SharedFile]

  implicit val serializedActorRefRW: ReadWriter[tasks.util.SerializedActorRef] =
    upickle.default.macroRW[tasks.util.SerializedActorRef]

}
