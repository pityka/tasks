package tasks.queue

import io.circe._
import io.circe.generic.semiauto._
 

case class TaskId(id: String, version: Int)
object TaskId {
  implicit val encoder: Encoder[TaskId] = deriveEncoder[TaskId]
  implicit val dec: Decoder[TaskId] = deriveDecoder[TaskId]
}

case class Base64Data(value: String) {}
object Base64Data {
  implicit val encoder: Encoder[Base64Data] = deriveEncoder[Base64Data]
  implicit val decoder: Decoder[Base64Data] = deriveDecoder[Base64Data]
}



case class TaskDescription(taskId: TaskId,
                           input: Base64Data,
                           persistentInput: Option[Base64Data]) 
object TaskDescription {
  implicit val encoder: Encoder[TaskDescription] =
    deriveEncoder[TaskDescription]
  implicit val dec: Decoder[TaskDescription] =
    deriveDecoder[TaskDescription]
}
