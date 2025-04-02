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
import tasks.util.message._
import tasks.util.message.MessageData.ScheduleTask
import tasks.util.message.MessageData.Schedule
import tasks.util.message.MessageData.NothingForSchedule
import tasks.util.message.LauncherName

private[tasks] trait Queue {
  def increment(launcher: LauncherName): IO[Unit]
  def scheduleTask(sch: ScheduleTask): IO[Unit]

  def initFailed(nodeName: RunningJobId): IO[Unit]

  def ack(
      allocated: VersionedResourceAllocated,
      launcher: LauncherName
  ): IO[Unit]

  def askForWork(
      launcherAsking: LauncherName,
      availableResources: VersionedResourceAvailable,
      node: Option[Node]
  ): IO[
    Either[Throwable, Either[NothingForSchedule.type, MessageData.Schedule]]
  ]

  def taskFailed(sch: ScheduleTask, cause: Throwable): IO[Unit]
  def taskSuccess(
      scheduleTask: ScheduleTask,
      receivedResult: UntypedResultWithMetadata,
      elapsedTime: ElapsedTimeNanoSeconds,
      resourceAllocated: ResourceAllocated
  ): IO[Unit]
}

private[tasks] final class QueueFromQueueImpl(
    queueImpl: QueueImpl
) extends Queue {

  def initFailed(nodeName: RunningJobId): IO[Unit] =
    queueImpl.initFailed(nodeName)

  def knownLaunchers = queueImpl.knownLaunchers.map(_.keySet)

  def increment(launcher: LauncherName): IO[Unit] =
    queueImpl.increment(launcher).attempt.map{
      case Right(value) =>  ()
      case Left(value) =>
          scribe.error("Unexpected failure. Handle error.",value)
          ()
    }
  def scheduleTask(sch: ScheduleTask): IO[Unit] = queueImpl.scheduleTask(sch)

  def ack(
      allocated: VersionedResourceAllocated,
      launcher: LauncherName
  ): IO[Unit] =
    queueImpl.ack(allocated, launcher)

  def askForWork(
      launcherAsking: LauncherName,
      availableResources: VersionedResourceAvailable,
      node: Option[Node]
  ): IO[
    Either[Throwable, Either[NothingForSchedule.type, MessageData.Schedule]]
  ] =
    queueImpl.askForWork(launcherAsking, availableResources, node).attempt

  def taskFailed(sch: ScheduleTask, cause: Throwable): IO[Unit] =
    queueImpl.taskFailed(sch, cause)
  def taskSuccess(
      scheduleTask: ScheduleTask,
      receivedResult: UntypedResultWithMetadata,
      elapsedTime: ElapsedTimeNanoSeconds,
      resourceAllocated: ResourceAllocated
  ): IO[Unit] = queueImpl.taskSuccess(
    scheduleTask,
    receivedResult,
    elapsedTime,
    resourceAllocated
  )
}

private[tasks] class QueueWithActor(
    queueActor: QueueActor,
    messenger: Messenger
) extends Queue {

  def initFailed(nodeName: RunningJobId): IO[Unit] =
    messenger.submit(
      Message(
        MessageData.InitFailed(nodeName),
        from = Address.unknown,
        to = queueActor.address0
      )
    )

  def increment(launcher: LauncherName): IO[Unit] =
    messenger.submit(
      Message(
        MessageData.Increment(launcher),
        from = Address.unknown,
        to = queueActor.address0
      )
    ).attempt.map{
      case Right(value) =>  ()
      case Left(value) =>
          scribe.error("Unexpected failure. Handle error.",value)
          ()
    }
  def ack(
      allocated: VersionedResourceAllocated,
      launcher: LauncherName
  ): IO[Unit] =
    messenger.submit(
      Message(
        MessageData.QueueAck(allocated, launcher),
        from = Address.unknown,
        to = queueActor.address0
      )
    )

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
      launcherAsking: LauncherName,
      availableResources: VersionedResourceAvailable,
      node: Option[Node]
  ): IO[
    Either[Throwable, Either[NothingForSchedule.type, MessageData.Schedule]]
  ] = {
    import scala.concurrent.duration._
    tasks.util.Ask
      .ask(
        target = queueActor.address0,
        data = MessageData.AskForWork(availableResources, launcherAsking, node),
        timeout = 1 minutes,
        messenger = messenger
      )
      .map {
        case Left(e) =>
          scribe.warn("timeout from queue", e)
          Left(e)
        case Right(None) =>
          Right(Left(NothingForSchedule))
        case Right(Some(Message(NothingForSchedule, _, _))) =>
          Right(Left(NothingForSchedule))
        case Right(Some(Message(sch @ Schedule(_), _, _))) =>
          Right(Right(sch))
        case Right(Some(Message(data, _, _))) =>
          scribe.warn(s"Unexpected answer from queue: $data")
          Right(Left(NothingForSchedule))
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
