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

trait Queue {
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

class QueueFromQueueImpl(
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

class QueueWithActor(
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

private[tasks] object TaskQueue {
  case class ScheduleTaskEqualityProjection(
      description: HashedTaskDescription
  )

  sealed trait Event
  case class Enqueued(sch: ScheduleTask, proxies: List[Proxy]) extends Event
  case class ProxyAddedToScheduledMessage(
      sch: ScheduleTask,
      proxies: List[Proxy]
  ) extends Event
  case class Negotiating(launcher: LauncherActor, sch: ScheduleTask)
      extends Event
  case class LauncherJoined(launcher: LauncherActor) extends Event
  case class FiberCreated(fiber: FiberIO[Unit]) extends Event
  case object NegotiationDone extends Event
  case class TaskScheduled(
      sch: ScheduleTask,
      launcher: LauncherActor,
      allocated: VersionedResourceAllocated
  ) extends Event
  case class TaskDone(
      sch: ScheduleTask,
      result: UntypedResultWithMetadata,
      elapsedTime: ElapsedTimeNanoSeconds,
      resourceAllocated: ResourceAllocated
  ) extends Event
  case class TaskFailed(sch: ScheduleTask) extends Event
  case class TaskLauncherStoppedFor(sch: ScheduleTask) extends Event
  case class LauncherCrashed(crashedLauncher: LauncherActor) extends Event
  case class CacheHit(sch: ScheduleTask, result: UntypedResult) extends Event

  def project(sch: ScheduleTask) =
    ScheduleTaskEqualityProjection(sch.description)

  case class State(
      queuedTasks: Map[
        ScheduleTaskEqualityProjection,
        (ScheduleTask, List[Proxy])
      ],
      scheduledTasks: Map[
        ScheduleTaskEqualityProjection,
        (LauncherActor, VersionedResourceAllocated, List[Proxy], ScheduleTask)
      ],
      knownLaunchers: Set[LauncherActor],
      /*This is non empty while waiting for response from the tasklauncher
       *during that, no other tasks are started*/
      negotiation: Option[(LauncherActor, ScheduleTask)],
      fibers: List[FiberIO[Unit]]
  ) {

    def update(e: Event): State = {
      e match {
        case FiberCreated(fiber) =>
          scribe.debug(s"FIBER CREATED $fiber")
          copy(fibers = fiber :: fibers)
        case Enqueued(sch, proxies) =>
          if (!scheduledTasks.contains(project(sch))) {
            queuedTasks.get(project(sch)) match {
              case None =>
                copy(
                  queuedTasks =
                    queuedTasks.updated(project(sch), (sch, proxies))
                )
              case Some((_, existingProxies)) =>
                copy(
                  queuedTasks.updated(
                    project(sch),
                    (sch, (proxies ::: existingProxies).distinct)
                  )
                )
            }
          } else update(ProxyAddedToScheduledMessage(sch, proxies))

        case ProxyAddedToScheduledMessage(sch, newProxies) =>
          val (launcher, allocation, proxies, _) = scheduledTasks(project(sch))
          copy(
            scheduledTasks = scheduledTasks
              .updated(
                project(sch),
                (launcher, allocation, (newProxies ::: proxies).distinct, sch)
              )
          )
        case Negotiating(launcher, sch) =>
          copy(negotiation = Some((launcher, sch)))
        case LauncherJoined(launcher) =>
          copy(knownLaunchers = knownLaunchers + launcher)
        case NegotiationDone => copy(negotiation = None)
        case TaskScheduled(sch, launcher, allocated) =>
          val (_, proxies) = queuedTasks(project(sch))
          copy(
            queuedTasks = queuedTasks - project(sch),
            scheduledTasks = scheduledTasks
              .updated(project(sch), (launcher, allocated, proxies, sch))
          )

        case TaskDone(sch, _, _, _) =>
          copy(scheduledTasks = scheduledTasks - project(sch))
        case TaskFailed(sch) =>
          copy(scheduledTasks = scheduledTasks - project(sch))
        case TaskLauncherStoppedFor(sch) =>
          copy(scheduledTasks = scheduledTasks - project(sch))
        case LauncherCrashed(launcher) =>
          copy(knownLaunchers = knownLaunchers - launcher)
        case CacheHit(sch, _) =>
          copy(scheduledTasks = scheduledTasks - project(sch))

      }
    }

    def queuedButSentByADifferentProxy(sch: ScheduleTask, proxy: Proxy) =
      (queuedTasks.contains(project(sch)) && (!queuedTasks(project(sch))._2
        .has(proxy)))

    def scheduledButSentByADifferentProxy(sch: ScheduleTask, proxy: Proxy) =
      scheduledTasks
        .get(project(sch))
        .map { case (_, _, proxies, _) =>
          !proxies.isEmpty && !proxies.contains(proxy)
        }
        .getOrElse(false)

    def negotiatingWithCurrentSender(sender: Address) =
      negotiation.map(_._1.address === sender).getOrElse(false)
  }

  object State {
    def empty = State(Map(), Map(), Set(), None, Nil)
  }

}

private[tasks] case class QueueActor(
    val address0: Address
)

private[tasks] object QueueActor {
  val singletonAddress = Address("TasksQueue")
  val referenceByAddress = QueueActor(singletonAddress)
  def makeReference(
      messenger: Messenger
  )(implicit config: TasksConfig): IO[Either[Throwable, QueueActor]] = Ask
    .ask(
      target = singletonAddress,
      data = MessageData.Ping,
      timeout = config.pendingNodeTimeout,
      messenger = messenger
    )
    .map {
      case Right(Some(_)) => (Right(referenceByAddress))
      case Right(None) => (
        Left(new RuntimeException(s"QueueActor not reachable"))
      )
      case Left(e) => Left(e)
    }

  def makeWithQueueImpl(
      impl: QueueImpl,
      cache: TaskResultCache,
      messenger: Messenger
  )(implicit
      config: TasksConfig
  ): Resource[IO, QueueActor] = {
    util.Actor.makeFromBehavior(
      new TaskQueue(impl, messenger, cache),
      messenger
    )
  }
}

private[tasks] final class TaskQueue(
    impl: QueueImpl,
    messenger: Messenger,
    cache: TaskResultCache
)(implicit config: TasksConfig)
    extends ActorBehavior[Unit, QueueActor](messenger) {
  val address: Address = QueueActor.singletonAddress
  val init: Unit = ()
  override def release(st: Unit) =
    impl.release
  def derive(
      ref: Ref[IO, Unit]
  ): QueueActor = QueueActor(address)
  import TaskQueue._

  def receive = (state, stateRef) => {
    case Message(sch: ScheduleTask, _, _) =>
      state -> impl.scheduleTask(sch)

    case Message(
          MessageData.AskForWork(availableResource, launcherAddress),
          from,
          _
        ) =>
      state -> impl
        .askForWork(LauncherActor(launcherAddress), availableResource)
        .flatMap {
          case Left(t) =>
            messenger.submit(
              Message(
                t,
                from = address,
                to = from
              )
            )
          case Right(t) =>
            messenger.submit(
              Message(
                t,
                from = address,
                to = from
              )
            )
        }

    case Message(MessageData.QueueAck(allocated, launcher), _, _) =>
      state -> impl.ack(allocated, launcher)

    case Message(
          MessageData.TaskDone(
            sch,
            resultWithMetadata,
            elapsedTime,
            resourceAllocated
          ),
          _,
          _
        ) =>
      state -> impl.taskSuccess(
        sch,
        resultWithMetadata,
        elapsedTime,
        resourceAllocated
      )

    case Message(MessageData.TaskFailedMessageToQueue(sch, cause), _, _) =>
      state -> impl.taskFailed(sch, cause)

    case Message(MessageData.Ping, from, to) =>
      state -> messenger.submit(
        Message(MessageData.Ping, from = address, to = from)
      )

    case Message(MessageData.HowLoadedAreYou, from, _) =>
      state -> impl.queryLoad.flatMap { qs =>
        messenger.submit(Message(qs, from = address, to = from))
      }

  }

}
