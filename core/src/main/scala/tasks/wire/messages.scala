package tasks.wire

import java.io.File

import akka.actor._

import tasks.util._
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

private[tasks] case class InternalMessageFromTask(
    actor: ActorRef,
    result: UntypedResultWithMetadata
) extends StaticMessage

private[tasks] case class InternalMessageTaskFailed(
    actor: ActorRef,
    cause: Throwable
) extends StaticMessage

private[tasks] case class FailureMessageFromProxyToProxy(cause: Throwable)
    extends StaticMessage

case class MessageFromTask(result: UntypedResult, retrievedFromCache: Boolean)
    extends StaticMessage

case object SaveDone extends StaticMessage

private[tasks] case class QueueAck(allocated: VersionedResourceAllocated)
    extends StaticMessage

private[tasks] case object GetMaximumSlots extends StaticMessage

private[tasks] case object GetAvailableSlots extends StaticMessage

case object YouShouldSetIt extends StaticMessage

case object Release extends StaticMessage

case class Schedule(sch: ScheduleTask) extends StaticMessage

// Messages related to files

case class GetSharedFolder(prefix: Vector[String]) extends StaticMessage

case class NewFile(f: File, p: ProposedManagedFilePath, ephemeralFile: Boolean)
    extends StaticMessage

case class NewSource(p: ProposedManagedFilePath) extends StaticMessage

case class AskForFile(p: ManagedFilePath, size: Long, hash: Int)
    extends StaticMessage

case object AckFileIsPresent extends StaticMessage

case class TransferToMe(actor: ActorRef) extends StaticMessage

case class TransferFileToUser(actor: ActorRef, sf: ManagedFilePath, fromOffset: Long)
    extends StaticMessage

case object WaitingForSharedFile extends StaticMessage

case object WaitingForPath extends StaticMessage

case class FileNotFound(e: Throwable) extends StaticMessage

case class Uploaded(
    length: Long,
    hash: Int,
    file: Option[File],
    p: ManagedFilePath
) extends StaticMessage

case class CouldNotUpload(p: ProposedManagedFilePath) extends StaticMessage

case class IsInStorageAnswer(value: Boolean) extends StaticMessage

case class ErrorWhileAccessingStore(e: Throwable) extends StaticMessage

case class NewRemote(uri: Uri) extends StaticMessage

case class IsAccessible(sf: ManagedFilePath, size: Long, hash: Int)
    extends StaticMessage

case class IsPathAccessible(sf: ManagedFilePath, retrieveSizeAndHash: Boolean)
    extends StaticMessage

case class GetUri(sf: ManagedFilePath) extends StaticMessage

case class Delete(sf: ManagedFilePath, size: Long, hash: Int)
    extends StaticMessage

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

// Messages related to cache

case class CacheActor(actor: ActorRef) extends StaticMessage

private[tasks] case class SaveResult(
    sch: HashedTaskDescription,
    r: UntypedResult,
    prefix: FileServicePrefix
) extends StaticMessage

private[tasks] case class CheckResult(sch: ScheduleTask, sender: Proxy)
    extends StaticMessage

private[tasks] case object PoisonPillToCacheActor extends StaticMessage

private[tasks] case object NeedInput extends StaticMessage

private[tasks] case class InputData(b64: Base64Data, noCache: Boolean)
    extends StaticMessage

private[tasks] case class AnswerFromCache(
    message: Either[String, Option[UntypedResult]],
    sender: Proxy,
    sch: ScheduleTask
) extends StaticMessage

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
