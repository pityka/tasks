package tasks.wire

import akka.serialization.Serializer
import akka.actor.ExtendedActorSystem
import tasks.queue.ScheduleTask

class StaticMessageSerializer(system: ExtendedActorSystem) extends Serializer {
  import io.circe.{Decoder, Encoder}

  implicit val as = system

  val log = akka.event.Logging(system.eventStream, "StaticMessageSerializer")

  override def identifier: Int = 999

  override def toBinary(o: AnyRef): Array[Byte] =
    o match {
      case t: StaticMessage =>
        val encoded = implicitly[Encoder[StaticMessage]]
          .apply(t)
          .noSpaces
          .getBytes("UTF-8")
        log.debug(s"Encoding {} as {}", t, encoded)
        encoded
    }

  override def includeManifest: Boolean = false

  override def fromBinary(bytes: Array[Byte],
                          manifest: Option[Class[_]]): AnyRef = {
    val r = io.circe.parser.decode[StaticMessage](new String(bytes)).right.get
    log.debug(s"Decoding {} as {}", new String(bytes), r)
    r
  }

}

class ScheduleTaskSerializer(system: ExtendedActorSystem) extends Serializer {
  import io.circe.{Decoder, Encoder}

  implicit val as = system

  val log = akka.event.Logging(system.eventStream, "ScheduleTaskSerializer")

  override def identifier: Int = 1000

  override def toBinary(o: AnyRef): Array[Byte] =
    o match {
      case t: ScheduleTask =>
        val encoded = implicitly[Encoder[ScheduleTask]]
          .apply(t)
          .noSpaces
          .getBytes("UTF-8")
        log.debug(s"Encoding {} as {}", t, encoded)
        encoded
    }

  override def includeManifest: Boolean = false

  override def fromBinary(bytes: Array[Byte],
                          manifest: Option[Class[_]]): AnyRef = {
    val r = io.circe.parser.decode[ScheduleTask](new String(bytes)).right.get
    log.debug(s"Decoding {} as {}", new String(bytes), r)
    r
  }

}
