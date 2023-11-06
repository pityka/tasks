package tasks.wire

import akka.actor._

import tasks.shared._
import tasks.fileservice._
import tasks.queue._
import tasks.elastic._

import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros._

sealed trait StaticMessage

// Messages related to the queue

private[tasks] case class QueryTask(sch: ScheduleTask, ac: ActorRef)
    extends StaticMessage

private[tasks] case class TaskDone(
    sch: ScheduleTask,
    result: UntypedResultWithMetadata,
    elapsedTime: ElapsedTimeNanoSeconds,
    resourceAllocated: ResourceAllocated
) extends StaticMessage

private[tasks] case class TaskFailedMessageToQueue(
    sch: ScheduleTask,
    cause: Throwable
) extends StaticMessage

private[tasks] case class TaskFailedMessageToProxy(
    sch: ScheduleTask,
    cause: Throwable
) extends StaticMessage

private[tasks] case class AskForWork(resources: VersionedResourceAvailable)
    extends StaticMessage

case object HowLoadedAreYou extends StaticMessage

private[tasks] case class FailureMessageFromProxyToProxy(cause: Throwable)
    extends StaticMessage

case class MessageFromTask(result: UntypedResult, retrievedFromCache: Boolean)
    extends StaticMessage

private[tasks] case class QueueAck(allocated: VersionedResourceAllocated)
    extends StaticMessage

private[tasks] case object GetMaximumSlots extends StaticMessage

private[tasks] case object GetAvailableSlots extends StaticMessage

case class Schedule(sch: ScheduleTask) extends StaticMessage

// Messages related to elastic

private[tasks] case object MeasureTime extends StaticMessage

private[tasks] case class Idling(state: Long) extends StaticMessage

private[tasks] case object Working extends StaticMessage

private[tasks] case object WhatAreYouDoing extends StaticMessage

private[tasks] case object PrepareForShutdown extends StaticMessage

private[tasks] case object ReadyForShutdown extends StaticMessage

private[tasks] case class NodeComingUp(node: Node) extends StaticMessage

private[tasks] case class InitFailed(nodename: PendingJobId)
    extends StaticMessage

private[tasks] case class RemoveNode(node: Node) extends StaticMessage

//

private[tasks] case object NeedInput extends StaticMessage

private[tasks] case class InputData(b64: Base64Data, noCache: Boolean)
    extends StaticMessage

// Messages related to HeartBeat

private[tasks] case object Ping extends StaticMessage

private[tasks] case object Pong extends StaticMessage

private[tasks] case class HeartBeatStopped(ac: ActorRef) extends StaticMessage

object StaticMessage {

  implicit def codec(implicit
      as: ExtendedActorSystem
  ): JsonValueCodec[StaticMessage] = {
    val _ = as
    JsonCodecMaker.make
  }

}
