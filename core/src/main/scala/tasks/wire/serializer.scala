package tasks.wire

import akka.serialization.Serializer
import akka.actor.ExtendedActorSystem

class CirceSerializer(system:ExtendedActorSystem) extends Serializer {
  import io.circe.{Decoder, Encoder}

  implicit val as = system

  override def identifier: Int = 999

  override def toBinary(o: AnyRef): Array[Byte] =
    o match {
      case t: StaticMessage =>
        implicitly[Encoder[StaticMessage]]
          .apply(t)
          .noSpaces
          .getBytes("UTF-8")
    }

  override def includeManifest: Boolean = false

  override def fromBinary(bytes: Array[Byte],
                          manifest: Option[Class[_]]): AnyRef =
    io.circe.parser.decode[StaticMessage](new String(bytes))

}
