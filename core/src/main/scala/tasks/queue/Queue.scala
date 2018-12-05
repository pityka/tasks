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
  case class TaskDone(sch: ScheduleTask, result: UntypedResult) extends Event
  case class TaskFailed(sch: ScheduleTask) extends Event
  case class TaskLauncherStoppedFor(sch: ScheduleTask) extends Event
  case class LauncherCrashed(crashedLauncher: LauncherActor) extends Event
  case class CacheQueried(sch: ScheduleTask) extends Event
  case class CacheHit(sch: ScheduleTask, result: UntypedResult) extends Event
}

class TaskQueue(eventListener: Option[EventListener[TaskQueue.Event]])(
    implicit config: TasksConfig)
    extends Actor
    with ActorLogging {

  import TaskQueue._

  case class State(
      queuedTasks: Map[ScheduleTask, List[Proxy]],
      scheduledTasks: Map[ScheduleTask,
                          (LauncherActor,
                           VersionedResourceAllocated,
                           List[Proxy])],
      knownLaunchers: Set[LauncherActor],
      /*This is non empty while waiting for response from the tasklauncher
       *during that, no other tasks are started*/
      negotiation: Option[(LauncherActor, ScheduleTask)]
  ) {

    def update(e: Event): State = {
      eventListener.foreach(_.receive(e))
      e match {
        case Enqueued(sch, proxies) =>
          if (!scheduledTasks.contains(sch)) {
            queuedTasks.get(sch) match {
              case None => copy(queuedTasks = queuedTasks.updated(sch, proxies))
              case Some(existingProxies) =>
                copy(
                  queuedTasks.updated(sch,
                                      (proxies ::: existingProxies).distinct))
            }
          } else update(ProxyAddedToScheduledMessage(sch, proxies))

        case ProxyAddedToScheduledMessage(sch, newProxies) =>
          val (launcher, allocation, proxies) = scheduledTasks(sch)
          copy(
            scheduledTasks = scheduledTasks
              .updated(
                sch,
                (launcher, allocation, (newProxies ::: proxies).distinct)))
        case Negotiating(launcher, sch) =>
          copy(negotiation = Some((launcher, sch)))
        case LauncherJoined(launcher) =>
          copy(knownLaunchers = knownLaunchers + launcher)
        case NegotiationDone => copy(negotiation = None)
        case TaskScheduled(sch, launcher, allocated) =>
          val proxies = queuedTasks(sch)
          copy(queuedTasks = queuedTasks - sch,
               scheduledTasks = scheduledTasks
                 .updated(sch, (launcher, allocated, proxies)))

        case TaskDone(sch, _) =>
          copy(scheduledTasks = scheduledTasks - sch)
        case TaskFailed(sch) =>
          copy(scheduledTasks = scheduledTasks - sch)
        case TaskLauncherStoppedFor(sch) =>
          copy(scheduledTasks = scheduledTasks - sch)
        case LauncherCrashed(launcher) =>
          copy(knownLaunchers = knownLaunchers - launcher)
        case CacheHit(sch, _) =>
          copy(scheduledTasks = scheduledTasks - sch)
        case _: CacheQueried => this

      }
    }

    def queuedButSentByADifferentProxy(sch: ScheduleTask, proxy: Proxy) =
      (queuedTasks.contains(sch) && (!queuedTasks(sch).has(proxy)))

    def scheduledButSentByADifferentProxy(sch: ScheduleTask, proxy: Proxy) =
      scheduledTasks
        .get(sch)
        .map {
          case (_, _, proxies) =>
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
          proxy.actor ! MessageFromTask(result)
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
          state.queuedTasks
            .map(x => (x._1.description.taskId, x._1.resource))
            .toSeq
        )

        val launcher = LauncherActor(sender)

        state.queuedTasks
          .filter {
            case (sch, _) =>
              val ret = availableResource.canFulfillRequest(sch.resource)
              if (!ret) {
                log.debug(
                  s"Can't fulfill request ${sch.resource} with available resources $availableResource")
              }
              ret
          }
          .toSeq
          .sortBy(_._1.priority.toInt)
          .headOption
          .foreach {
            case (sch, proxies) =>
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

              launcher.actor ! ScheduleWithProxy(sch, proxies)

          }
      } else {
        log.debug("AskForWork received but currently in negotiation state.")
      }

    case Ack(allocated) if state.negotiatingWithCurrentSender =>
      val sch = state.negotiation.get._2
      if (state.scheduledTasks.contains(sch)) {
        log.error(
          "Routed messages already contains task. This is unexpected and can lead to lost messages.")
      }
      context.become(
        running(
          state
            .update(NegotiationDone)
            .update(TaskScheduled(sch, LauncherActor(sender), allocated))))

    case wire.TaskDone(sch, result) =>
      log.debug(s"TaskDone $sch $result")

      state.scheduledTasks.get(sch).foreach {
        case (_, _, proxies) =>
          proxies.foreach(_.actor ! MessageFromTask(result))
      }
      context.become(running(state.update(TaskDone(sch, result))))

      if (state.queuedTasks.contains(sch)) {
        log.error("Should not be queued. {}", state.queuedTasks(sch))
      }

    case TaskFailedMessageToQueue(sch, cause) =>
      val updated = state.scheduledTasks.get(sch).foldLeft(state) {
        case (state, (_, _, proxies)) =>
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
      val updated = msgs.foldLeft(state) { (state, sch) =>
        val proxies = state.scheduledTasks(sch)._3
        state.update(TaskLauncherStoppedFor(sch)).update(Enqueued(sch, proxies))
      }
      context.become(running(updated.update(LauncherCrashed(launcher))))
      log.info(
        "Requeued " + msgs.size + " messages. Queue size: " + updated.queuedTasks.keys.size)

    case Ping =>
      sender ! Pong

    case HowLoadedAreYou =>
      val qs = QueueStat(
        state.queuedTasks.toList
          .map(_._1)
          .map(x => (x.description.taskId.toString, x.resource))
          .toList,
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
