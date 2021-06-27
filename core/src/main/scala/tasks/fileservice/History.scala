package tasks.fileservice

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

sealed trait HistoryContext
case class HistoryContextImpl(
    task: History.TaskVersion,
    codeVersion: String,
    traceId: Option[String]
) extends HistoryContext

case object NoHistory extends HistoryContext

case class History(self: SharedFile, context: Option[HistoryContext])

object History {

  implicit val ordering: Ordering[History] = Ordering.by(_.self.toString)

  case class TaskVersion(taskID: String, taskVersion: Int)
  object TaskVersion {
    implicit val codec: JsonValueCodec[TaskVersion] = JsonCodecMaker.make

  }

  implicit val codec: JsonValueCodec[History] = JsonCodecMaker.make

}

object HistoryContext {
  implicit val codec: JsonValueCodec[HistoryContext] = JsonCodecMaker.make
}
