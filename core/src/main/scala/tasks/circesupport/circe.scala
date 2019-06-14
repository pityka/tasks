package tasks
import tasks.queue._
import com.typesafe.scalalogging.StrictLogging

package object circesupport extends StrictLogging {
  import io.circe.{Encoder, Decoder, Printer}
  private val printer = Printer.noSpaces.copy(dropNullValues = true)

  implicit def serializer[A](implicit enc: Encoder[A]): Serializer[A] =
    new Serializer[A] {
      def apply(a: A) = printer.pretty(enc(a)).getBytes("UTF-8")
    }
  implicit def deserializer[A](implicit dec: Decoder[A]): Deserializer[A] =
    new Deserializer[A] {
      def apply(b: Array[Byte]) =
        io.circe.parser.decode[A](new String(b)).left.map(_.toString)
    }

  implicit val sharedFileDecoder =
    io.circe.generic.semiauto.deriveDecoder[tasks.fileservice.SharedFile]

  implicit val sharedFileEncoder =
    io.circe.generic.semiauto.deriveEncoder[tasks.fileservice.SharedFile]

}
