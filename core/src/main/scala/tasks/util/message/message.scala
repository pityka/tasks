package tasks.util.message

import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.core.JsonWriter
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.github.plokhotnyuk.jsoniter_scala.core.JsonReader

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
  def apply(value: String): Address = Address(value, None)
}

case class Message(data: MessageData, from: Address, to: Address)
object Message {

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

sealed trait MessageData
private[tasks] object MessageData {
  import tasks.shared._
  import tasks.queue._
  case object Ping extends MessageData
  case object HowLoadedAreYou extends MessageData
  case class AskForWork(resources: VersionedResourceAvailable)
      extends MessageData
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
  case class QueueStat(
      queued: List[(String, VersionedResourceRequest)],
      running: List[(String, VersionedResourceAllocated)]
  ) extends MessageData
  case object PrepareForShutdown extends MessageData
  case object WhatAreYouDoing extends MessageData
  case class ScheduleTask(
      description: HashedTaskDescription,
      inputDeserializer: Spore[AnyRef, AnyRef],
      outputSerializer: Spore[AnyRef, AnyRef],
      function: Spore[AnyRef, AnyRef],
      resource: VersionedResourceRequest,
      queueActor: QueueActor,
      fileServicePrefix: FileServicePrefix,
      tryCache: Boolean,
      priority: Priority,
      labels: Labels,
      lineage: TaskLineage,
      proxy: Address
  ) extends MessageData
  case class InitFailed(nodename: PendingJobId) extends MessageData
  case class TaskFailedMessageToQueue(
      sch: ScheduleTask,
      cause: Throwable
  ) extends MessageData
  case class Schedule(sch: ScheduleTask) extends MessageData
  case class QueueAck(allocated: VersionedResourceAllocated) extends MessageData
  case object NothingForSchedule extends MessageData
  case object NeedInput extends MessageData
  case object Working extends MessageData
  case class Idling(idleState: Long) extends MessageData
  case class RemoveNode(node: tasks.elastic.Node) extends MessageData
  case class NodeComingUp(node: tasks.elastic.Node) extends MessageData
  case object ReadyForShutdown extends MessageData
  case class InputData(b64: Base64Data, noCache: Boolean) extends MessageData

  case class MessageFromTask(result: UntypedResult, retrievedFromCache: Boolean)
      extends MessageData
}
