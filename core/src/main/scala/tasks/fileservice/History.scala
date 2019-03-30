package tasks.fileservice

import io.circe.generic.semiauto._
import io.circe.{Encoder, Decoder}

sealed trait HistoryContext
case class HistoryContextImpl(
    task: History.TaskVersion,
    codeVersion: String
) extends HistoryContext

case object NoHistory extends HistoryContext

case class History(self: SharedFile, context: Option[HistoryContext])

object History {

  implicit val ordering: Ordering[History] = Ordering.by(_.self.toString)

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
  implicit val encoder: Encoder[HistoryContext] =
    deriveEncoder[HistoryContext]
  implicit val decoder: Decoder[HistoryContext] =
    deriveDecoder[HistoryContext]
}
