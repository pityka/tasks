package tasks.fileservice

import io.circe.generic.semiauto._
import io.circe.{Encoder, Decoder}

sealed trait HistoryContext {
  def dependencies: List[History]
}
case class HistoryContextImpl(
    dependencies: List[History],
    task: History.TaskVersion,
    codeVersion: String,
    timestamp: java.time.Instant
) extends HistoryContext
case object NoHistory extends HistoryContext {
  def dependencies = Nil
}

case class History(self: SharedFile, context: Option[HistoryContext])

object History {

  case class TaskVersion(taskID: String, taskVersion: Int)
  object TaskVersion {
    implicit val encoder: Encoder[TaskVersion] =
      deriveEncoder[TaskVersion]
    implicit val decoder: Decoder[TaskVersion] =
      deriveDecoder[TaskVersion]
  }

  implicit val encoder: Encoder[History] =
    deriveEncoder[History]
  implicit val decoder: Decoder[History] =
    deriveDecoder[History]

}

object HistoryContext {
  implicit val instantEncoder =
    Encoder.encodeLong.contramap[java.time.Instant](i => i.toEpochMilli)

  implicit val instantDecoder =
    Decoder.decodeLong.map(i => java.time.Instant.ofEpochMilli(i))

  implicit val encoder: Encoder[HistoryContext] =
    deriveEncoder[HistoryContext]
  implicit val decoder: Decoder[HistoryContext] =
    deriveDecoder[HistoryContext]
}
