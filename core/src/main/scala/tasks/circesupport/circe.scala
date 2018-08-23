package tasks
import tasks.queue._

package object circesupport {
  import io.circe.{Encoder, Decoder}
  implicit def serializer[A](implicit enc: Encoder[A]): Serializer[A] =
    new Serializer[A] {
      def apply(a: A) = enc(a).noSpaces.getBytes("UTF-8")
    }
  implicit def deserielizer[A](implicit dec: Decoder[A]): Deserializer[A] =
    new Deserializer[A] {
      def apply(b: Array[Byte]) =
        io.circe.parser.decode[A](new String(b)).right.get
    }

  implicit val sharedFileDecoder =
    io.circe.generic.semiauto.deriveDecoder[tasks.fileservice.SharedFile]

  implicit val sharedFileEncoder =
    io.circe.generic.semiauto.deriveEncoder[tasks.fileservice.SharedFile]
}
