package tasks.queue

import com.google.common.hash.Hashing
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import java.nio.charset.StandardCharsets

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
    Hashing.murmur3_128.hashBytes(value).toString
  )
}

object SerializedTaskDescription {
  def hash(td: TaskDescription): HashedTaskDescription = {
    val hasher = Hashing.murmur3_128.newHasher()
    val base64TaskDescription = td.persistentInput.getOrElse(td.input).value
    hasher.putString((td.taskId.toString + "\n"), StandardCharsets.US_ASCII)
    hasher.putString(base64TaskDescription, StandardCharsets.US_ASCII)
    HashedTaskDescription(hasher.hash().toString)

  }
  def apply(td: TaskDescription): SerializedTaskDescription = {
    val base64TaskDescription =
      td.persistentInput.getOrElse(td.input).value

    SerializedTaskDescription(
      (td.taskId.toString + "\n" + base64TaskDescription)
        .getBytes(StandardCharsets.US_ASCII)
    )
  }
}
