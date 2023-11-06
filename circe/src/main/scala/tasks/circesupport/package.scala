package tasks
import tasks.queue._
import io.circe.generic.semiauto._
import io.circe.{Encoder, Decoder, Printer}
import tasks.fileservice.{FilePath, RemoteFilePath, ManagedFilePath}
import tasks.util.Uri
import com.google.common.hash.Hashing
import tasks.util.SerializedActorRef

package object circesupport {
  private val printer = Printer.noSpaces.copy(dropNullValues = true)

  implicit def serializer[A](implicit enc: Encoder[A]): Serializer[A] =
    new Serializer[A] {

      override def hash(a: A): String = {
        Hashing.murmur3_128.hashBytes(apply(a)).toString
      }

      def apply(a: A) = printer.print(enc(a)).getBytes("UTF-8")
    }
  implicit def deserializer[A](implicit dec: Decoder[A]): Deserializer[A] =
    new Deserializer[A] {

      def apply(b: Array[Byte]) =
        io.circe.parser.decode[A](new String(b)).left.map(_.toString)
    }

  implicit val decoder4: Decoder[Uri] = deriveDecoder[Uri]
  implicit val encoder4: Encoder[Uri] = deriveEncoder[Uri]
  implicit val decoder1: Decoder[FilePath] = deriveDecoder[FilePath]
  implicit val encoder1: Encoder[FilePath] = deriveEncoder[FilePath]

  implicit val decoder2: Decoder[RemoteFilePath] = deriveDecoder[RemoteFilePath]
  implicit val encoder2: Encoder[RemoteFilePath] = deriveEncoder[RemoteFilePath]

  implicit val decoder3: Decoder[ManagedFilePath] =
    deriveDecoder[ManagedFilePath]
  implicit val encoder3: Encoder[ManagedFilePath] =
    deriveEncoder[ManagedFilePath]

  implicit val sharedFileDecoder: Decoder[SharedFile] =
    deriveDecoder[tasks.fileservice.SharedFile]

  implicit val sharedFileEncoder: Encoder[SharedFile] =
    io.circe.generic.semiauto.deriveEncoder[tasks.fileservice.SharedFile]

  implicit val serializedActorRefEncoder: Encoder[SerializedActorRef] =
    io.circe.generic.semiauto.deriveEncoder[tasks.util.SerializedActorRef]
  implicit val serializedActorRefDecoder: Decoder[SerializedActorRef] =
    io.circe.generic.semiauto.deriveDecoder[tasks.util.SerializedActorRef]

}
