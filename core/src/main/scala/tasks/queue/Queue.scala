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
import tasks.shared.monitor._
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
    val address: Address
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

  def makeWithRunloop(cache: TaskResultCache, messenger: Messenger)(implicit
      config: TasksConfig
  ): Resource[IO, QueueActor] = {
    util.Actor.makeFromBehavior(new TaskQueue(messenger, cache), messenger)
  }
}

private[tasks] final class TaskQueue(
    messenger: Messenger,
    cache: TaskResultCache
)(implicit config: TasksConfig)
    extends ActorBehavior[TaskQueue.State, QueueActor](messenger) {
  val address: Address = QueueActor.singletonAddress
  val init: TaskQueue.State = TaskQueue.State.empty
  override def release(st: TaskQueue.State) =
    IO(
      scribe.debug(
        s"Releasing resources held by TasksQueue: ${st.fibers} fibers"
      )
    ) *> IO.parSequenceN(1)(st.fibers.map(_.cancel)).void
  def derive(
      ref: Ref[IO, TaskQueue.State]
  ): QueueActor = QueueActor(address)
  import TaskQueue._

  private def handleCacheAnwser(
      ref: Ref[IO, State],
      a: tasks.caching.AnswerFromCache
  ): IO[Unit] = {
    val message = a.message
    val proxy = a.sender
    val sch = a.sch
    scribe.debug("Cache answered.")
    message match {
      case Right(Some(result)) => {
        scribe.debug("Replying with a Result found in cache.")
        ref.flatModify { state =>
          state.update(CacheHit(sch, result)) ->
            messenger.submit(
              Message(
                MessageData.MessageFromTask(result, retrievedFromCache = true),
                from = address,
                to = proxy.address
              )
            )

        }
      }
      case Right(None) => {
        scribe.debug("Task is not found in cache. Enqueue. ")
        ref.update(_.update(Enqueued(sch, List(proxy))))
      }
      case Left(_) => {
        scribe.debug("Task is not found in cache. Enqueue. ")
        ref.update(_.update(Enqueued(sch, List(proxy))))
      }

    }
  }
  private def handleLauncherStopped(
      launcher: LauncherActor,
      stateRef: Ref[IO, State]
  ) = stateRef.update { state =>
    scribe.info(s"LauncherStopped: $launcher")
    val msgs =
      state.scheduledTasks.toSeq.filter(_._2._1 === launcher).map(_._1)
    val updated = msgs.foldLeft(state) { (state, schProjection) =>
      val (_, _, proxies, sch) = state.scheduledTasks(schProjection)
      state.update(TaskLauncherStoppedFor(sch)).update(Enqueued(sch, proxies))
    }

    val negotiatingWithStoppedLauncher = state.negotiation.exists {
      case (negotiatingLauncher, _) =>
        (negotiatingLauncher: LauncherActor) == (launcher: LauncherActor)
    }
    if (negotiatingWithStoppedLauncher) {
      scribe.error(
        "Launcher stopped during negotiation phase. Automatic recovery from this is not implemented. The scheduler is deadlocked and it should be restarted."
      )
    }
    updated.update(LauncherCrashed(launcher))
  }
  def receive = (state, stateRef) => {
    case Message(sch: ScheduleTask, from, to) =>
      scribe.debug("Received ScheduleTask.")
      val proxy = Proxy(from)

      if (state.queuedButSentByADifferentProxy(sch, proxy)) {
        state.update(Enqueued(sch, List(proxy))) -> IO.unit
      } else if (state.scheduledButSentByADifferentProxy(sch, proxy)) {
        scribe.debug(
          s"Scheduletask received multiple times from different proxies. Not queueing this one, but delivering result if ready. $sch"
        )
        state.update(ProxyAddedToScheduledMessage(sch, List(proxy))) -> IO.unit

      } else {
        if (sch.tryCache) {
          state -> cache
            .checkResult(sch, proxy)
            .flatMap(r => handleCacheAnwser(stateRef, r))
            .start
            .void
        } else {
          scribe.debug(
            "ScheduleTask should not be checked in the cache. Enqueue. "
          )
          state.update(Enqueued(sch, List(proxy))) -> IO.unit
        }

      }

    case Message(MessageData.AskForWork(availableResource), from, to) =>
      if (state.negotiation.isEmpty) {
        scribe.debug(
          s"AskForWork ${from} $availableResource ${state.negotiation} ${state.queuedTasks.map { case (_, (sch, _)) =>
              (sch.description.taskId, sch.resource)
            }.toSeq}"
        )

        val launcher = LauncherActor(from)

        var maxPrio = Int.MinValue
        var selected = Option.empty[ScheduleTask]
        state.queuedTasks.valuesIterator
          .foreach { case (sch, _) =>
            val ret = availableResource.canFulfillRequest(sch.resource)
            if (!ret && (maxPrio == Int.MinValue || sch.priority.s > maxPrio)) {
              scribe.debug(
                s"Can't fulfill request ${sch.resource} with available resources $availableResource or lower priority than an already selected task"
              )
            } else {
              maxPrio = sch.priority.s
              selected = Some(sch)
            }

          }

        selected match {
          case None =>
            state -> messenger.submit(
              Message(
                MessageData.NothingForSchedule,
                from = address,
                to = launcher.address
              )
            )

          case Some(sch) =>
            val withNegotiation = state.update(Negotiating(launcher, sch))
            scribe.debug(
              s"Dequeued task ${sch.description.taskId.id} ${sch.description.dataHash} with priority ${sch.priority}. Sending task to $launcher. (Negotation state of queue: ${state.negotiation})"
            )

            val (newState, io1) =
              if (!state.knownLaunchers.contains(launcher)) {
                val st2 = withNegotiation.update(LauncherJoined(launcher))
                st2 -> HeartBeatIO
                  .make(
                    target = launcher.address,
                    sideEffect = handleLauncherStopped(launcher, stateRef),
                    messenger = messenger
                  )
                  .start
                  .flatMap { heartBeatFiber =>
                    stateRef.update(_.update(FiberCreated(heartBeatFiber)))

                  }
              } else withNegotiation -> IO.unit

            val io2 = messenger.submit(
              Message(
                MessageData.Schedule(sch),
                from = address,
                to = launcher.address
              )
            )

            newState -> (io1 *> io2)

        }

      } else {
        state -> IO(
          scribe.debug(
            "AskForWork received but currently in negotiation state."
          )
        )
      }

    case Message(MessageData.QueueAck(allocated), from, to)
        if state.negotiatingWithCurrentSender(from) =>
      val sch = state.negotiation.get._2
      if (state.scheduledTasks.contains(project(sch))) {
        scribe.error(
          "Routed messages already contains task. This is unexpected and can lead to lost messages."
        )
      }

      state
        .update(NegotiationDone)
        .update(TaskScheduled(sch, LauncherActor(from), allocated)) -> IO.unit

    case Message(
          MessageData.TaskDone(
            sch,
            resultWithMetadata,
            elapsedTime,
            resourceAllocated
          ),
          from,
          to
        ) =>
      scribe.debug(s"TaskDone $sch $resultWithMetadata")
      if (state.queuedTasks.contains(project(sch))) {
        scribe.error(
          s"Should not be queued. ${state.queuedTasks(project(sch))}"
        )
      }
      val io = IO
        .parSequenceN(1)(
          state.scheduledTasks
            .get(project(sch))
            .toList
            .flatMap { case (_, _, proxies, _) =>
              proxies.toList.map { pr =>
                messenger.submit(
                  Message(
                    MessageData.MessageFromTask(
                      resultWithMetadata.untypedResult,
                      retrievedFromCache = false
                    ),
                    from = address,
                    to = pr.address
                  )
                )

              }

            }
            .toList
        )
        .void

      state.update(
        TaskDone(sch, resultWithMetadata, elapsedTime, resourceAllocated)
      ) -> io

    case Message(MessageData.TaskFailedMessageToQueue(sch, cause), from, to) =>
      val (updated, sideEffects) = state.scheduledTasks
        .get(project(sch))
        .foldLeft((state, List.empty[IO[Unit]])) {
          case ((state, sideEffectAcc), (_, _, proxies, _)) =>
            val removed = state.update(TaskFailed(sch))
            if (config.resubmitFailedTask) {
              scribe.error(
                cause,
                "Task execution failed ( resubmitting infinite time until done): " + sch.toString
              )
              scribe.info(
                "Requeued 1 message. Queue size: " + state.queuedTasks.keys.size
              )
              (removed.update(Enqueued(sch, proxies)), sideEffectAcc)
            } else {
              val sideEffects = proxies.map(pr =>
                messenger.submit(
                  Message(
                    MessageData.TaskFailedMessageToProxy(sch, cause),
                    from = address,
                    to = pr.address
                  )
                )
              )
              scribe.error(cause, "Task execution failed: " + sch.toString)
              (removed, sideEffectAcc ++ sideEffects)
            }
        }
      updated -> IO.parSequenceN(1)(sideEffects).void

    case Message(MessageData.Ping, from, to) =>
      state -> messenger.submit(
        Message(MessageData.Ping, from = address, to = from)
      )

    case Message(MessageData.HowLoadedAreYou, from, to) =>
      val qs = MessageData.QueueStat(
        state.queuedTasks.toList.map { case (_, (sch, _)) =>
          (sch.description.taskId.toString, sch.resource)
        }.toList,
        state.scheduledTasks.toSeq
          .map(x => x._1.description.taskId.toString -> x._2._2)
          .toList
      )

      state -> messenger.submit(Message(qs, from = address, to = from))

  }

}
