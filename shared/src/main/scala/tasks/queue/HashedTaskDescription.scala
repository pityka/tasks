package tasks.queue

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

case class TaskId(id: String, version: Int)
object TaskId {
  implicit val codec: JsonValueCodec[TaskId] = JsonCodecMaker.make
}

case class Base64Data(value: String) {}
object Base64Data {
  implicit val codec: JsonValueCodec[Base64Data] = JsonCodecMaker.make
}
case class HashedTaskDescription(taskId: TaskId, dataHash: String) {
  def hash = s"${taskId.id}-${taskId.version}-$dataHash"
}

object HashedTaskDescription {
  implicit def toLogFeature(rm: HashedTaskDescription): scribe.LogFeature =
    scribe.data(
      Map(
        "hashed-taskdescription-task-id" -> s"${rm.taskId.id}.${rm.taskId.version}",
        "hashed-taskdescription-data-hash" -> rm.dataHash
      )
    )
  implicit val codec: JsonValueCodec[HashedTaskDescription] =
    JsonCodecMaker.make

}
