/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Modified work, Copyright (c) 2016 Istvan Bartha
 * Modified work, Copyright (c) 2018 Istvan Bartha

 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package tasks.queue

import tasks.util.eq._
import tasks.shared._
import tasks.util._
import tasks.util.config._
import tasks.wire._
import tasks._
import tasks.caching.TaskResultCache
import cats.effect.IO
import tasks.caching.AnswerFromCache
import cats.effect.FiberIO
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import tasks.util.Actor.ActorBehavior
import tasks.elastic.RemoteNodeRegistry
import tasks.util.message._
import tasks.util.message.MessageData.ScheduleTask
import tasks.util.message.MessageData.Schedule
import tasks.util.message.MessageData.NothingForSchedule
import tasks.queue.Launcher.LauncherActor

private[tasks] trait Queue {
  def queryLoad: IO[Option[MessageData.QueueStat]]
  def ping: IO[Unit]
  def scheduleTask(sch: ScheduleTask): IO[Unit]

  def ack(allocated: VersionedResourceAllocated, launcher: Address): IO[Unit]

  def askForWork(
      launcherAsking: LauncherActor,
      availableResources: VersionedResourceAvailable
  ): IO[Either[NothingForSchedule.type, MessageData.Schedule]]

  def taskFailed(sch: ScheduleTask, cause: Throwable): IO[Unit]
  def taskSuccess(
      scheduleTask: ScheduleTask,
      receivedResult: UntypedResultWithMetadata,
      elapsedTime: ElapsedTimeNanoSeconds,
      resourceAllocated: ResourceAllocated
  ): IO[Unit]
}

private[tasks] class QueueFromQueueImpl(
  queueImpl: QueueImpl
) extends Queue {
   def queryLoad: IO[Option[MessageData.QueueStat]] = queueImpl.queryLoad.map(Some(_))
  def ping: IO[Unit] = IO.unit
  def scheduleTask(sch: ScheduleTask): IO[Unit] = queueImpl.scheduleTask(sch)

  def ack(allocated: VersionedResourceAllocated, launcher: Address): IO[Unit] = 
      queueImpl.ack(allocated,launcher)

  def askForWork(
      launcherAsking: LauncherActor,
      availableResources: VersionedResourceAvailable
  ): IO[Either[NothingForSchedule.type, MessageData.Schedule]] = 
      queueImpl.askForWork(launcherAsking,availableResources)

  def taskFailed(sch: ScheduleTask, cause: Throwable): IO[Unit] = 
      queueImpl.taskFailed(sch,cause)
  def taskSuccess(
      scheduleTask: ScheduleTask,
      receivedResult: UntypedResultWithMetadata,
      elapsedTime: ElapsedTimeNanoSeconds,
      resourceAllocated: ResourceAllocated
  ): IO[Unit] = queueImpl.taskSuccess(scheduleTask,receivedResult,elapsedTime,resourceAllocated)
}

private[tasks] class QueueWithActor(
    queueActor: QueueActor,
    messenger: Messenger
) extends Queue {
  def ack(allocated: VersionedResourceAllocated, launcher: Address): IO[Unit] =
    messenger.submit(
      Message(
        MessageData.QueueAck(allocated, launcher),
        from = Address.unknown,
        to = queueActor.address0
      )
    )
  def queryLoad: IO[Option[tasks.util.message.MessageData.QueueStat]] = {
    import scala.concurrent.duration._
    tasks.util.Ask
      .ask(
        target = queueActor.address0,
        data = MessageData.HowLoadedAreYou,
        timeout = 1 minutes,
        messenger = messenger
      )
      .flatMap {
        case Left(e) =>
          scribe.warn("HowLoadedAreYou timed out")
          IO.pure(None)
        case Right(None) =>
          scribe.warn("HowLoadedAreYou timed out")
          IO.pure(None)
        case Right(Some(Message(q @ MessageData.QueueStat(_, _), _, _))) =>
          IO.pure(Some(q))
        case x =>
          scribe.error(s"HowLoadedAreYou got unexpected response $x")
          IO.pure(None)
      }
  }
  def ping: IO[Unit] = {
    import scala.concurrent.duration._
    tasks.util.Ask
      .ask(
        target = queueActor.address0,
        data = MessageData.Ping,
        timeout = 1 minutes,
        messenger = messenger
      )
      .flatMap {
        case Left(e) => IO.raiseError(e)
        case Right(None) =>
          IO.raiseError(new RuntimeException("Ping got no response"))
        case Right(Some(Message(MessageData.Ping, _, _))) => IO.unit
        case x =>
          scribe.error(s"Ping got unexpected response $x")
          IO.raiseError(new RuntimeException(s"Ping got unexpeced response $x"))
      }
  }
  def scheduleTask(sch: ScheduleTask): IO[Unit] =
    messenger
      .submit(
        Message(
          from = Address.unknown,
          to = queueActor.address0,
          data = sch
        )
      )
  def askForWork(
      launcherAsking: LauncherActor,
      availableResources: VersionedResourceAvailable
  ): IO[Either[NothingForSchedule.type, MessageData.Schedule]] = {
    import scala.concurrent.duration._
    tasks.util.Ask
      .ask(
        target = queueActor.address0,
        data =
          MessageData.AskForWork(availableResources, launcherAsking.address),
        timeout = 1 minutes,
        messenger = messenger
      )
      .map {
        case Left(e) =>
          scribe.warn("timeout from queue", e)
          Left(NothingForSchedule)
        case Right(None) =>
          Left(NothingForSchedule)
        case Right(Some(Message(NothingForSchedule, _, _))) =>
          Left(NothingForSchedule)
        case Right(Some(Message(sch @ Schedule(_), _, _))) =>
          Right(sch)
        case Right(Some(Message(data, _, _))) =>
          scribe.warn(s"Unexpected answer from queue: $data")
          Left(NothingForSchedule)
      }

  }
  def taskFailed(sch: ScheduleTask, cause: Throwable): IO[Unit] =
    messenger
      .submit(
        Message(
          from = Address.unknown,
          to = queueActor.address0,
          data = MessageData.TaskFailedMessageToQueue(sch, cause)
        )
      )
  def taskSuccess(
      scheduleTask: ScheduleTask,
      receivedResult: UntypedResultWithMetadata,
      elapsedTime: ElapsedTimeNanoSeconds,
      resourceAllocated: ResourceAllocated
  ): IO[Unit] =
    messenger
      .submit(
        Message(
          from = Address.unknown,
          to = queueActor.address0,
          data = MessageData.TaskDone(
            scheduleTask,
            receivedResult,
            elapsedTime,
            resourceAllocated
          )
        )
      )
}


