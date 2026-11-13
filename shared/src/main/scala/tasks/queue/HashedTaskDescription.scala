package tasks.queue

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

sealed trait TaskId {
  def id: String
  def version: Int
}
object TaskId {
  private[tasks] final case class TaskIdImpl(id: String, version: Int)
      extends TaskId

  def apply(id: String, version: Int): TaskId = TaskIdImpl(id, version)

  implicit val codec: JsonValueCodec[TaskId] = {
    val innerCodec: JsonValueCodec[TaskIdImpl] = JsonCodecMaker.make
    new JsonValueCodec[TaskId] {
      val nullValue: TaskId = null
      def encodeValue(x: TaskId, out: JsonWriter): Unit =
        innerCodec.encodeValue(x.asInstanceOf[TaskIdImpl], out)
      def decodeValue(in: JsonReader, default: TaskId): TaskId =
        innerCodec.decodeValue(in, null)
    }
  }
}

private[tasks] case class Base64Data(value: String)
private[tasks] object Base64Data {
  implicit val codec: JsonValueCodec[Base64Data] = JsonCodecMaker.make
}

sealed trait HashedTaskDescription {
  def taskId: TaskId
  def dataHash: String
  final def hash = s"${taskId.id}-${taskId.version}-$dataHash"
}

object HashedTaskDescription {
  private[tasks] final case class HashedTaskDescriptionImpl(
      taskId: TaskId,
      dataHash: String
  ) extends HashedTaskDescription

  def apply(taskId: TaskId, dataHash: String): HashedTaskDescription =
    HashedTaskDescriptionImpl(taskId, dataHash)

  implicit def toLogFeature(rm: HashedTaskDescription): scribe.LogFeature =
    scribe.data(
      Map(
        "hashed-taskdescription-task-id" -> s"${rm.taskId.id}.${rm.taskId.version}",
        "hashed-taskdescription-data-hash" -> rm.dataHash
      )
    )
  implicit val codec: JsonValueCodec[HashedTaskDescription] = {
    val innerCodec: JsonValueCodec[HashedTaskDescriptionImpl] =
      JsonCodecMaker.make
    new JsonValueCodec[HashedTaskDescription] {
      val nullValue: HashedTaskDescription = null
      def encodeValue(x: HashedTaskDescription, out: JsonWriter): Unit =
        innerCodec.encodeValue(
          x.asInstanceOf[HashedTaskDescriptionImpl],
          out
        )
      def decodeValue(
          in: JsonReader,
          default: HashedTaskDescription
      ): HashedTaskDescription =
        innerCodec.decodeValue(in, null)
    }
  }

}
