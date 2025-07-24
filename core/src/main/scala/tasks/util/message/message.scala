package tasks.util.message

import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.core.JsonWriter
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.github.plokhotnyuk.jsoniter_scala.core.JsonReader

case class Node(
    name: tasks.shared.RunningJobId,
    size: tasks.shared.ResourceAvailable,
    launcherActor: LauncherName
)
object Node {
  implicit def toLogFeature(rm: Node): scribe.LogFeature = scribe.data(
    Map(
      "node-name" -> rm.name.value,
      "node-launcher-name" -> rm.launcherActor.name,
      "node-available-cpu" -> rm.size.cpu,
      "node-available-memory" -> rm.size.memory,
      "node-available-gpu" -> rm.size.gpu.mkString(","),
      "node-available-scratch" -> rm.size.scratch,
      "node-available-image" -> rm.size.image.getOrElse("none")
    )
  )
}

case class LauncherName(name: String)
object LauncherName {
  implicit def toLogFeature(rm: LauncherName): scribe.LogFeature = scribe.data(
    Map(
      "launcher-name" -> rm.name
    )
  )
}

case class Address(value: String, listeningUri: Option[String]) {
  private[util] def withoutUri = Address(value, None)
  def withAddress(s: Option[String]) =
    copy(listeningUri = listeningUri.orElse(s))
  override def equals(that: Any): Boolean = {
    that match {
      case Address(v, _) => value == v
      case _             => false
    }
  }

  override def hashCode(): Int = value.hashCode()
}
object Address {
  implicit def toLogFeature(rm: Address): scribe.LogFeature = scribe.data(
    Map(
      "address-name" -> rm.value,
      "address-listening-uri" -> rm.listeningUri.getOrElse("none")
    )
  )
  val unknown = Address("unknown", None)
  def apply(value: String): Address = Address(value, None)
}

case class Message(data: MessageData, from: Address, to: Address)
object Message {

  implicit def toLogFeature(m: Message): scribe.LogFeature = scribe.data(
    Map(
      "from-uri" -> m.from.listeningUri.getOrElse("none"),
      "from-name" -> m.from.value,
      "to-uri" -> m.to.listeningUri.getOrElse("none"),
      "to-name" -> m.to.value,
      "data-class" -> m.data.getClass().getCanonicalName()
    )
  )

  implicit val throwableCodec: JsonValueCodec[Throwable] = {
    type DTO = (String, List[(String, String, String, Int)])
    val codec0: JsonValueCodec[DTO] = JsonCodecMaker.make

    new JsonValueCodec[Throwable] {
      val nullValue: Throwable = null.asInstanceOf[Throwable]

      def encodeValue(
          throwable: Throwable,
          out: JsonWriter
      ): _root_.scala.Unit = {
        val dto = (
          throwable.getMessage,
          throwable.getStackTrace.toList.map(stackTraceElement =>
            (
              stackTraceElement.getClassName,
              stackTraceElement.getMethodName,
              stackTraceElement.getFileName,
              stackTraceElement.getLineNumber
            )
          )
        )
        codec0.encodeValue(dto, out)
      }

      def decodeValue(in: JsonReader, default: Throwable): Throwable = {
        val (msg, stackTrace) = codec0.decodeValue(in, codec0.nullValue)
        val exc = new Exception(msg)
        exc.setStackTrace(stackTrace.map { case (cls, method, file, line) =>
          new java.lang.StackTraceElement(cls, method, file, line)
        }.toArray)
        exc
      }
    }
  }

  implicit val codec: JsonValueCodec[Message] =
    com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker.make
}

import tasks.fileservice.FileServicePrefix

case class QueueStat(
    queued: List[(String, tasks.shared.VersionedResourceRequest)],
    running: List[(String, tasks.shared.VersionedResourceAllocated)]
)

sealed trait MessageData
private[tasks] object MessageData {
  import tasks.shared._
  import tasks.queue._
  case object Ping extends MessageData
  case class AskForWork(
      resources: VersionedResourceAvailable,
      launcher: LauncherName,
      node: Option[Node]
  ) extends MessageData
  case class TaskDone(
      sch: ScheduleTask,
      result: UntypedResultWithMetadata,
      elapsedTime: ElapsedTimeNanoSeconds,
      resourceAllocated: ResourceAllocated
  ) extends MessageData
  case class TaskFailedMessageToProxy(
      sch: ScheduleTask,
      cause: Throwable
  ) extends MessageData

  case class InputData(b64: Base64Data, noCache: Boolean)
  case class ScheduleTask(
      description: HashedTaskDescription,
      inputDeserializer: Spore[AnyRef, AnyRef],
      outputSerializer: Spore[AnyRef, AnyRef],
      function: Spore[AnyRef, AnyRef],
      resource: VersionedResourceRequest,
      input: InputData,
      fileServicePrefix: FileServicePrefix,
      tryCache: Boolean,
      priority: Priority,
      labels: Labels,
      lineage: TaskLineage,
      proxy: Address
  ) extends MessageData
  object ScheduleTask {
    implicit def toLogFeature(rm: ScheduleTask): scribe.LogFeature =
      scribe.data(
        Map(
          "sch-descr" -> s"${rm.description.taskId.id}.${rm.description.taskId.version}",
          "sch-resource-request-cpu" -> s"${rm.resource.cpuMemoryRequest.cpu}",
          "sch-resource-request-memory" -> s"${rm.resource.cpuMemoryRequest.memory}",
          "sch-resource-request-gpu" -> s"${rm.resource.cpuMemoryRequest.gpu}",
          "sch-resource-request-scratch" -> s"${rm.resource.cpuMemoryRequest.scratch}",
          "sch-resource-request-image" -> s"${rm.resource.cpuMemoryRequest.image}",
          "sch-input-hash" -> s"${rm.description.dataHash}",
          "proxy-name" -> rm.proxy.value,
          "proxy-uri" -> rm.proxy.listeningUri.getOrElse("none")
        )
      )
  }
  case class InitFailed(nodename: RunningJobId) extends MessageData
  case class TaskFailedMessageToQueue(
      sch: ScheduleTask,
      cause: Throwable
  ) extends MessageData
  case class Schedule(sch: ScheduleTask) extends MessageData
  case class Increment(launcher: LauncherName) extends MessageData
  case class QueueAck(
      allocated: VersionedResourceAllocated,
      launcher: LauncherName
  ) extends MessageData
  case object NothingForSchedule extends MessageData

  case class MessageFromTask(result: UntypedResult, retrievedFromCache: Boolean)
      extends MessageData
}
