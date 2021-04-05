package tasks

import tasks.queue._

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

  implicit val instantRW =
    upickle.default
      .readwriter[Long]
      .bimap[java.time.Instant](
        instant => instant.toEpochMilli,
        { case num =>
          java.time.Instant.ofEpochMilli(num)
        }
      )

  implicit val uri =
    upickle.default.macroRW[tasks.util.Uri]

  implicit val rpath =
    upickle.default.macroRW[tasks.fileservice.RemoteFilePath]

  implicit val mpath =
    upickle.default.macroRW[tasks.fileservice.ManagedFilePath]

  implicit val filepath =
    upickle.default.macroRW[tasks.fileservice.FilePath]

  implicit val sharedFileRW =
    upickle.default.macroRW[tasks.fileservice.SharedFile]

}
