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

import akka.actor.{Actor, ActorLogging}

import tasks.util.eq._
import tasks.shared._
import tasks.shared.monitor._
import tasks.util._
import tasks.util.config._
import tasks.wire._
import tasks._
import tasks.ui.EventListener

object TaskQueue {
  sealed trait Event
  case class Enqueued(sch: ScheduleTask, proxies: List[Proxy]) extends Event
  case class ProxyAddedToScheduledMessage(sch: ScheduleTask,
                                          proxies: List[Proxy])
      extends Event
  case class Negotiating(launcher: LauncherActor, sch: ScheduleTask)
      extends Event
  case class LauncherJoined(launcher: LauncherActor) extends Event
  case object NegotiationDone extends Event
  case class TaskScheduled(sch: ScheduleTask,
                           launcher: LauncherActor,
                           allocated: VersionedResourceAllocated)
      extends Event
  case class TaskDone(sch: ScheduleTask,
                      result: UntypedResultWithMetadata,
                      elapsedTime: ElapsedTimeNanoSeconds,
                      resourceAllocated: ResourceAllocated)
      extends Event
  case class TaskFailed(sch: ScheduleTask) extends Event
  case class TaskLauncherStoppedFor(sch: ScheduleTask) extends Event
  case class LauncherCrashed(crashedLauncher: LauncherActor) extends Event
  case class CacheQueried(sch: ScheduleTask) extends Event
  case class CacheHit(sch: ScheduleTask, result: UntypedResult) extends Event
}

class TaskQueue(eventListener: Seq[EventListener[TaskQueue.Event]])(
    implicit config: TasksConfig)
    extends Actor
    with ActorLogging {

  import TaskQueue._

  case class ScheduleTaskEqualityProjection(
      description: TaskDescription
  )

  def project(sch: ScheduleTask) =
    ScheduleTaskEqualityProjection(sch.description)

  case class State(
      queuedTasks: Map[ScheduleTaskEqualityProjection,
                       (ScheduleTask, List[Proxy])],
      scheduledTasks: Map[ScheduleTaskEqualityProjection,
                          (LauncherActor,
                           VersionedResourceAllocated,
                           List[Proxy],
                           ScheduleTask)],
      knownLaunchers: Set[LauncherActor],
      /*This is non empty while waiting for response from the tasklauncher
       *during that, no other tasks are started*/
      negotiation: Option[(LauncherActor, ScheduleTask)]
  ) {

    def update(e: Event): State = {
      eventListener.foreach(_.receive(e))
      e match {
        case Enqueued(sch, proxies) =>
          if (!scheduledTasks.contains(project(sch))) {
            queuedTasks.get(project(sch)) match {
              case None =>
                copy(
                  queuedTasks =
                    queuedTasks.updated(project(sch), (sch, proxies)))
              case Some((_, existingProxies)) =>
                copy(
                  queuedTasks.updated(
                    project(sch),
                    (sch, (proxies ::: existingProxies).distinct)))
            }
          } else update(ProxyAddedToScheduledMessage(sch, proxies))

        case ProxyAddedToScheduledMessage(sch, newProxies) =>
          val (launcher, allocation, proxies, _) = scheduledTasks(project(sch))
          copy(
            scheduledTasks = scheduledTasks
              .updated(
                project(sch),
                (launcher, allocation, (newProxies ::: proxies).distinct, sch)))
        case Negotiating(launcher, sch) =>
          copy(negotiation = Some((launcher, sch)))
        case LauncherJoined(launcher) =>
          copy(knownLaunchers = knownLaunchers + launcher)
        case NegotiationDone => copy(negotiation = None)
        case TaskScheduled(sch, launcher, allocated) =>
          val (_, proxies) = queuedTasks(project(sch))
          copy(queuedTasks = queuedTasks - project(sch),
               scheduledTasks = scheduledTasks
                 .updated(project(sch), (launcher, allocated, proxies, sch)))

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
        case _: CacheQueried => this

      }
    }

    def queuedButSentByADifferentProxy(sch: ScheduleTask, proxy: Proxy) =
      (queuedTasks.contains(project(sch)) && (!queuedTasks(project(sch))._2
        .has(proxy)))

    def scheduledButSentByADifferentProxy(sch: ScheduleTask, proxy: Proxy) =
      scheduledTasks
        .get(project(sch))
        .map {
          case (_, _, proxies, _) =>
            !proxies.isEmpty && !proxies.contains(proxy)
        }
        .getOrElse(false)

    def negotiatingWithCurrentSender =
      negotiation.map(_._1.actor === sender()).getOrElse(false)
  }

  object State {
    def empty = State(Map(), Map(), Set(), None)
  }

  def running(state: State): Receive = {
    case sch: ScheduleTask =>
      log.debug("Received ScheduleTask.")
      val proxy = Proxy(sender)

      if (state.queuedButSentByADifferentProxy(sch, proxy)) {
        context.become(running(state.update(Enqueued(sch, List(proxy)))))
      } else if (state.scheduledButSentByADifferentProxy(sch, proxy)) {
        log.debug(
          "Scheduletask received multiple times from different proxies. Not queueing this one, but delivering result if ready. {}",
          sch)
        context.become(
          running(state.update(ProxyAddedToScheduledMessage(sch, List(proxy)))))
      } else {
        if (sch.tryCache) {
          sch.cacheActor ! CheckResult(sch, proxy)
          context.become(running(state.update(CacheQueried(sch))))
        } else {
          log.debug(
            "ScheduleTask should not be checked in the cache. Enqueue. ")
          context.become(running(state.update(Enqueued(sch, List(proxy)))))
        }

      }

    case AnswerFromCache(message, proxy, sch) =>
      log.debug("Cache answered.")
      message match {
        case Right(Some(result)) => {
          log.debug("Replying with a Result found in cache.")
          context.become(running(state.update(CacheHit(sch, result))))
          proxy.actor ! MessageFromTask(result, retrievedFromCache = true)
        }
        case Right(None) => {
          log.debug("Task is not found in cache. Enqueue. ")
          context.become(running(state.update(Enqueued(sch, List(proxy)))))
        }
        case Left(_) => {
          log.debug("Task is not found in cache. Enqueue. ")
          context.become(running(state.update(Enqueued(sch, List(proxy)))))
        }

      }

    case AskForWork(availableResource) =>
      if (state.negotiation.isEmpty) {
        log.debug(
          "AskForWork. Sender: {}. Resource: {}. Negotition state: {}. Queue state: {}",
          sender,
          availableResource,
          state.negotiation,
          state.queuedTasks.map {
            case (_, (sch, _)) => (sch.description.taskId, sch.resource)
          }.toSeq
        )

        val launcher = LauncherActor(sender)

        state.queuedTasks
          .filter {
            case (_, (sch, _)) =>
              val ret = availableResource.canFulfillRequest(sch.resource)
              if (!ret) {
                log.debug(
                  s"Can't fulfill request ${sch.resource} with available resources $availableResource")
              }
              ret
          }
          .toSeq
          .sortBy {
            case (_, (sch, _)) =>
              sch.priority.toInt
          }
          .headOption
          .foreach {
            case (_, (sch, _)) =>
              val withNegotiation = state.update(Negotiating(launcher, sch))
              log.debug(
                s"Dequeued. Sending task to $launcher. Negotation: ${state.negotiation}")

              val newState = if (!state.knownLaunchers.contains(launcher)) {
                HeartBeatActor.watch(launcher.actor,
                                     LauncherStopped(launcher),
                                     self)
                withNegotiation.update(LauncherJoined(launcher))
              } else withNegotiation

              context.become(running(newState))

              launcher.actor ! Schedule(sch)

          }
      } else {
        log.debug("AskForWork received but currently in negotiation state.")
      }

    case Ack(allocated) if state.negotiatingWithCurrentSender =>
      val sch = state.negotiation.get._2
      if (state.scheduledTasks.contains(project(sch))) {
        log.error(
          "Routed messages already contains task. This is unexpected and can lead to lost messages.")
      }
      context.become(
        running(
          state
            .update(NegotiationDone)
            .update(TaskScheduled(sch, LauncherActor(sender), allocated))))

    case wire.TaskDone(sch,
                       resultWithMetadata,
                       elapsedTime,
                       resourceAllocated) =>
      log.debug(s"TaskDone $sch $resultWithMetadata")

      state.scheduledTasks.get(project(sch)).foreach {
        case (_, _, proxies, _) =>
          proxies.foreach(
            _.actor ! MessageFromTask(resultWithMetadata.untypedResult,
                                      retrievedFromCache = false))
      }
      context.become(
        running(
          state.update(
            TaskDone(sch, resultWithMetadata, elapsedTime, resourceAllocated))))

      if (state.queuedTasks.contains(project(sch))) {
        log.error("Should not be queued. {}", state.queuedTasks(project(sch)))
      }

    case TaskFailedMessageToQueue(sch, cause) =>
      val updated = state.scheduledTasks.get(project(sch)).foldLeft(state) {
        case (state, (_, _, proxies, _)) =>
          val removed = state.update(TaskFailed(sch))
          if (config.resubmitFailedTask) {
            log.error(
              cause,
              "Task execution failed ( resubmitting infinite time until done): " + sch.toString)
            log.info(
              "Requeued 1 message. Queue size: " + state.queuedTasks.keys.size)
            removed.update(Enqueued(sch, proxies))
          } else {
            proxies.foreach(_.actor ! TaskFailedMessageToProxy(sch, cause))
            log.error(cause, "Task execution failed: " + sch.toString)
            removed
          }
      }
      context.become(running(updated))

    case LauncherStopped(launcher) =>
      log.info(s"LauncherStopped: $launcher")
      val msgs =
        state.scheduledTasks.toSeq.filter(_._2._1 === launcher).map(_._1)
      val updated = msgs.foldLeft(state) { (state, schProjection) =>
        val (_, _, proxies, sch) = state.scheduledTasks(schProjection)
        state.update(TaskLauncherStoppedFor(sch)).update(Enqueued(sch, proxies))
      }
      context.become(running(updated.update(LauncherCrashed(launcher))))
      log.info(
        "Requeued " + msgs.size + " messages. Queue size: " + updated.queuedTasks.keys.size)

      val negotiatingWithStoppedLauncher = state.negotiation.exists {
        case (negotiatingLauncher, _) =>
          (negotiatingLauncher: LauncherActor) == (launcher: LauncherActor)
      }
      if (negotiatingWithStoppedLauncher) {
        log.error(
          "Launcher stopped during negotiation phase. Automatic recovery from this is not implemented. The scheduler is deadlocked and it should be restarted.")
      }

    case Ping =>
      sender ! Pong

    case HowLoadedAreYou =>
      val qs = QueueStat(
        state.queuedTasks.toList.map {
          case (_, (sch, _)) => (sch.description.taskId.toString, sch.resource)
        }.toList,
        state.scheduledTasks.toSeq
          .map(x => x._1.description.taskId.toString -> x._2._2)
          .toList
      )

      sender ! qs

  }

  override def preStart: Unit = {
    log.debug("TaskQueue starting.")
    context.become(running(State.empty))
  }

  override def postStop: Unit = {
    log.info("TaskQueue stopped.")
  }

  def receive: Receive = {
    case _ => ???
  }
}
