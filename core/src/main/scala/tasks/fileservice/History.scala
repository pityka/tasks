package tasks.fileservice

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

sealed trait HistoryContext
private[tasks] case class HistoryContextImpl(
    task: History.TaskVersion,
    codeVersion: String,
    traceId: Option[String]
) extends HistoryContext

case object NoHistory extends HistoryContext

sealed trait History {
  def self: SharedFile
  def context: Option[HistoryContext]
}

object History {

  private[tasks] final case class HistoryImpl(
      self: SharedFile,
      context: Option[HistoryContext]
  ) extends History

  def apply(self: SharedFile, context: Option[HistoryContext]): History =
    HistoryImpl(self, context)

  implicit val ordering: Ordering[History] = Ordering.by(_.self.toString)

  private[tasks] case class TaskVersion(taskID: String, taskVersion: Int)
  private[tasks] object TaskVersion {
    implicit val codec: JsonValueCodec[TaskVersion] = JsonCodecMaker.make

  }

  implicit val codec: JsonValueCodec[History] = {
    val innerCodec: JsonValueCodec[HistoryImpl] = JsonCodecMaker.make
    new JsonValueCodec[History] {
      val nullValue: History = null
      def encodeValue(x: History, out: JsonWriter): Unit =
        innerCodec.encodeValue(x.asInstanceOf[HistoryImpl], out)
      def decodeValue(in: JsonReader, default: History): History =
        innerCodec.decodeValue(in, null)
    }
  }

}

object HistoryContext {
  implicit val codec: JsonValueCodec[HistoryContext] = JsonCodecMaker.make
}
