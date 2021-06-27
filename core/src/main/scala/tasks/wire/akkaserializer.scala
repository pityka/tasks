package tasks.wire

import akka.serialization.Serializer
import akka.actor.ExtendedActorSystem
import tasks.queue.ScheduleTask
import tasks.fileservice.SharedFile
import com.github.plokhotnyuk.jsoniter_scala.core._

class StaticMessageSerializer(system: ExtendedActorSystem) extends Serializer {

  implicit val as = system

  val log =
    akka.event.Logging(system.eventStream, "tasks.wire.StaticMessageSerializer")

  override def identifier: Int = 999

  override def toBinary(o: AnyRef): Array[Byte] =
    o match {
      case t: StaticMessage =>
        val encoded = writeToArray(t, WriterConfig)
        log.debug(s"Encoding {} as {}", t, encoded)
        encoded
    }

  override def includeManifest: Boolean = false

  override def fromBinary(
      bytes: Array[Byte],
      manifest: Option[Class[_]]
  ): AnyRef = {
    val r = readFromArray[StaticMessage](bytes)
    log.debug(s"Decoding {} as {}", new String(bytes), r)
    r
  }

}

class ScheduleTaskSerializer(system: ExtendedActorSystem) extends Serializer {

  implicit val as = system
  implicit val schCodec = ScheduleTask.codec(system)

  val log =
    akka.event.Logging(system.eventStream, "tasks.wire.ScheduleTaskSerializer")

  override def identifier: Int = 1000

  override def toBinary(o: AnyRef): Array[Byte] =
    o match {
      case t: ScheduleTask =>
        val encoded = writeToArray(t, WriterConfig)

        log.debug(s"Encoding {} as {}", t, encoded)
        encoded
    }

  override def includeManifest: Boolean = false

  override def fromBinary(
      bytes: Array[Byte],
      manifest: Option[Class[_]]
  ): AnyRef = {

    val r = readFromArray[ScheduleTask](bytes)
    log.debug(s"Decoding {} as {}", new String(bytes), r)
    r
  }

}
class SharedFileSerializer(system: ExtendedActorSystem) extends Serializer {

  implicit val as = system

  val log =
    akka.event.Logging(system.eventStream, "tasks.wire.SharedFileSerializer")

  override def identifier: Int = 1001

  override def toBinary(o: AnyRef): Array[Byte] =
    o match {
      case t: SharedFile =>
        val encoded = writeToArray(t, WriterConfig)
        log.debug(s"Encoding {} as {}", t, encoded)
        encoded
    }

  override def includeManifest: Boolean = false

  override def fromBinary(
      bytes: Array[Byte],
      manifest: Option[Class[_]]
  ): AnyRef = {
    val r = readFromArray[SharedFile](bytes)
    log.debug(s"Decoding {} as {}", new String(bytes), r)
    r
  }

}
