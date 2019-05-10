package tasks.queue

import com.google.common.hash.Hashing
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._

case class HashedTaskDescription(hash: String) {
  override def toString = hash
}

object HashedTaskDescription {
  implicit val encoder: Encoder[HashedTaskDescription] =
    deriveEncoder[HashedTaskDescription]

  implicit val decoder: Decoder[HashedTaskDescription] =
    deriveDecoder[HashedTaskDescription]
}

case class SerializedTaskDescription(value: Array[Byte]) {
  val hash = HashedTaskDescription(
    Hashing.murmur3_128.hashBytes(value).toString)
}

object SerializedTaskDescription {
  def apply(td: TaskDescription): SerializedTaskDescription = {
    val base64TaskDescription =
      td.persistentInput.getOrElse(td.input).value

    SerializedTaskDescription(
      (td.taskId + "\n" + base64TaskDescription).getBytes("UTF8"))
  }
}
