package tasks.wire

import java.io.File

import akka.actor._

import io.circe.generic.semiauto._
import io.circe._

import cats.syntax.either._

import tasks._
import tasks.util._
import tasks.shared._
import tasks.fileservice._
import tasks.queue._
import tasks.elastic._


case class Save(s: String, v: Any) extends java.io.Serializable

sealed trait StaticMessage extends java.io.Serializable

// Messages related to the queue

case class LookUp(s: String) extends StaticMessage

case object ATaskWasForwarded extends StaticMessage

case class QueueInfo(q: List[(ScheduleTask, List[ActorRef])])
    extends StaticMessage

private[tasks] case object GetQueueInformation extends StaticMessage

private[tasks] case class QueryTask(sch: ScheduleTask, ac: ActorRef)
    extends StaticMessage

private[tasks] case class TaskDone(sch: ScheduleTask, result: UntypedResult)
    extends StaticMessage

private[tasks] case class TaskFailedMessageToQueue(sch: ScheduleTask,
                                                   cause: Throwable)
    extends StaticMessage

private[tasks] case class TaskFailedMessageToProxy(sch: ScheduleTask,
                                                   cause: Throwable)
    extends StaticMessage

private[tasks] case class AskForWork(resources: CPUMemoryAvailable)
    extends StaticMessage

case object HowLoadedAreYou extends StaticMessage

case object GetBackResult extends StaticMessage

private[tasks] case class InternalMessageFromTask(actor: ActorRef,
                                                  result: UntypedResult)  extends StaticMessage

private[tasks] case class InternalMessageTaskFailed(actor: ActorRef,
                                                    cause: Throwable)  extends StaticMessage

private[tasks] case class FailureMessageFromProxyToProxy(cause: Throwable)  extends StaticMessage

case class MessageFromTask(result: UntypedResult)  extends StaticMessage

case object SaveDone  extends StaticMessage

private[tasks] case class Ack(allocated: CPUMemoryAllocated)  extends StaticMessage

private[tasks] case class RegisterForNotification(actor: ActorRef)  extends StaticMessage

private[tasks] case object GetMaximumSlots  extends StaticMessage

private[tasks] case object GetAvailableSlots  extends StaticMessage

case object YouShouldSetIt extends StaticMessage

case object Release  extends StaticMessage

case class ScheduleWithProxy(sch: ScheduleTask, ac: List[ActorRef])    extends StaticMessage

// Messages related to files

case class GetListOfFilesInStorage(regexp: String)  extends StaticMessage

case class NewFile(f: File, p: ProposedManagedFilePath, ephemeralFile: Boolean)
      extends StaticMessage

case class NewSource(p: ProposedManagedFilePath)  extends StaticMessage

case class GetPaths(p: ManagedFilePath, size: Long, hash: Int)
     extends StaticMessage

case class KnownPaths(paths: List[File])  extends StaticMessage

case class TransferToMe(actor: ActorRef)  extends StaticMessage

case class TransferFileToUser(actor: ActorRef, sf: ManagedFilePath)  extends StaticMessage

case object WaitingForSharedFile  extends StaticMessage

case object WaitingForPath  extends StaticMessage

case class FileNotFound(e: Throwable) extends StaticMessage

case class Uploaded(length: Long,
                    hash: Int,
                    file: Option[File],
                    p: ManagedFilePath)
     extends StaticMessage

case class CouldNotUpload(p: ProposedManagedFilePath) extends StaticMessage

case class IsInStorageAnswer(value: Boolean)  extends StaticMessage

case class ErrorWhileAccessingStore(e: Throwable)  extends StaticMessage

case class NewRemote(uri: Uri)  extends StaticMessage

case class IsAccessible(sf: ManagedFilePath, size: Long, hash: Int) extends StaticMessage

case class GetUri(sf: ManagedFilePath)  extends StaticMessage

// Messages related to elastic

case object GetNodeRegistryStat  extends StaticMessage

private[tasks] case object MeasureTime  extends StaticMessage

private[tasks] case class Idling(state: Long) extends StaticMessage

private[tasks] case object Working  extends StaticMessage

private[tasks] case object WhatAreYouDoing   extends StaticMessage

private[tasks] case object PrepareForShutdown  extends StaticMessage

private[tasks] case object ReadyForShutdown  extends StaticMessage

private[tasks] case class NodeComingUp(node: Node) extends StaticMessage

private[tasks] case class InitFailed(nodename: PendingJobId)  extends StaticMessage

private[tasks] case class NodeIsDown(node: Node)  extends StaticMessage

// Messages related to cache

case class CacheActor(actor: ActorRef)  extends StaticMessage

private[tasks] case class SaveResult(sch: TaskDescription,
                                     r: UntypedResult,
                                     prefix: FileServicePrefix)
  extends StaticMessage

private[tasks] case class CheckResult(sch: ScheduleTask, sender: ActorRef)
      extends StaticMessage

private[tasks] case object PoisonPillToCacheActor   extends StaticMessage

private[tasks] case class AnswerFromCache(
    message: Either[String, Option[UntypedResult]],
    sender: ActorRef,
    sch: ScheduleTask)
      extends StaticMessage


      // Messages related to HeartBeat

      private[tasks] case object Ping  extends StaticMessage

      private[tasks] case object Pong  extends StaticMessage

      private[tasks] case class HeartBeatStopped(ac: ActorRef)  extends StaticMessage



object StaticMessage {
  import io.circe.disjunctionCodecs._
  implicit val encode: Encoder[StaticMessage] = deriveEncoder[StaticMessage]
  implicit def decoder(implicit as:ExtendedActorSystem) : Decoder[StaticMessage] = deriveDecoder[StaticMessage]
}
