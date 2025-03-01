package tasks.util
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
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
