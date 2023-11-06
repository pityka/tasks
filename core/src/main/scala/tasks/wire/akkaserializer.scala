package tasks.wire

import akka.serialization.Serializer
import akka.actor.ExtendedActorSystem
import tasks.queue.ScheduleTask
import tasks.fileservice.SharedFile
import com.github.plokhotnyuk.jsoniter_scala.core._

class StaticMessageSerializer(system: ExtendedActorSystem) extends Serializer {

  implicit val as : ExtendedActorSystem = system


  override def identifier: Int = 999

  override def toBinary(o: AnyRef): Array[Byte] =
    o match {
      case t: StaticMessage =>
        val encoded = writeToArray(t, WriterConfig)
        scribe.debug(s"Encoding $t as $encoded")
        encoded
    }

  override def includeManifest: Boolean = false

  override def fromBinary(
      bytes: Array[Byte],
      manifest: Option[Class[_]]
  ): AnyRef = {
    val r = readFromArray[StaticMessage](bytes)
    scribe.debug(s"Decoding ${new String(bytes)} as $r")
    r
  }

}

class ScheduleTaskSerializer(system: ExtendedActorSystem) extends Serializer {

  implicit val as  : ExtendedActorSystem = system
  implicit val schCodec : JsonValueCodec[ScheduleTask]= ScheduleTask.codec(system)


  override def identifier: Int = 1000

  override def toBinary(o: AnyRef): Array[Byte] =
    o match {
      case t: ScheduleTask =>
        val encoded = writeToArray(t, WriterConfig)

        scribe.debug(s"Encoding $t as $encoded")
        encoded
    }

  override def includeManifest: Boolean = false

  override def fromBinary(
      bytes: Array[Byte],
      manifest: Option[Class[_]]
  ): AnyRef = {

    val r = readFromArray[ScheduleTask](bytes)
    scribe.debug(s"Decoding ${new String(bytes)} as $r")
    r
  }

}
class SharedFileSerializer(system: ExtendedActorSystem) extends Serializer {

  implicit val as : ExtendedActorSystem= system

  override def identifier: Int = 1001

  override def toBinary(o: AnyRef): Array[Byte] =
    o match {
      case t: SharedFile =>
        val encoded = writeToArray(t, WriterConfig)
        scribe.debug(s"Encoding $t as $encoded")
        encoded
    }

  override def includeManifest: Boolean = false

  override def fromBinary(
      bytes: Array[Byte],
      manifest: Option[Class[_]]
  ): AnyRef = {
    val r = readFromArray[SharedFile](bytes)
    scribe.debug(s"Decoding ${new String(bytes)} as $r")
    r
  }

}
