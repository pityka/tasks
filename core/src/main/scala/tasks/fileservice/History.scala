package tasks.fileservice

import io.circe.generic.semiauto._
import io.circe.{Encoder, Decoder}

case class History(dependencies: List[SharedFile],
                   task: History.TaskVersion,
                   timestamp: java.time.Instant,
                   codeVersion: String)

object History {

  case class TaskVersion(taskID: String, taskVersion: Int)
  object TaskVersion {
    implicit val encoder: Encoder[TaskVersion] =
      deriveEncoder[TaskVersion]
    implicit val decoder: Decoder[TaskVersion] =
      deriveDecoder[TaskVersion]
  }

  implicit val instantEncoder =
    Encoder.encodeLong.contramap[java.time.Instant](i => i.toEpochMilli)

  implicit val instantDecoder =
    Decoder.decodeLong.map(i => java.time.Instant.ofEpochMilli(i))

  implicit val encoder: Encoder[History] =
    deriveEncoder[History]
  implicit val decoder: Decoder[History] =
    deriveDecoder[History]
}
