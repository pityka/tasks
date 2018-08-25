/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Modified work, Copyright (c) 2016 Istvan Bartha

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

class TaskQueue(implicit config: TasksConfig) extends Actor with ActorLogging {

  private val queuedTasks =
    collection.mutable.Map[ScheduleTask, List[Proxy]]()

  private val scheduledMessages = scala.collection.mutable
    .Map[ScheduleTask,
         (LauncherActor, VersionedCPUMemoryAllocated, List[Proxy])]()

  // This is non empty while waiting for response from the tasklauncher
  // during that, no other tasks are started
  private var negotiation: Option[(LauncherActor, ScheduleTask)] = None

  private def negotiatingWithCurrentSender =
    negotiation.map(_._1.actor === sender).getOrElse(false)

  private val knownLaunchers = scala.collection.mutable.HashSet[LauncherActor]()

  private def enQueue(sch: ScheduleTask, proxies: List[Proxy]): Unit =
    if (!scheduledMessages.contains(sch)) {
      queuedTasks.get(sch) match {
        case None => queuedTasks.update(sch, proxies)
        case Some(existingProxies) =>
          queuedTasks.update(sch, (proxies ::: existingProxies).distinct)
      }
    } else addProxiesToScheduledMessages(sch, proxies)

  private def removeFromScheduledMessages(sch: ScheduleTask): Unit = {
    scheduledMessages.remove(sch)
  }

  private def taskDone(sch: ScheduleTask, r: UntypedResult): Unit = {
    scheduledMessages.get(sch).foreach {
      case (_, _, proxies) =>
        proxies.foreach(_.actor ! MessageFromTask(r))
    }
    removeFromScheduledMessages(sch)

    if (queuedTasks.contains(sch)) {
      log.error("Should not be queued. {}", queuedTasks(sch))
    }
  }

  private def taskFailed(sch: ScheduleTask, cause: Throwable): Unit =
    scheduledMessages.get(sch).foreach {
      case (_, _, proxies) =>
        scheduledMessages.remove(sch)
        if (config.resubmitFailedTask) {
          log.error(
            cause,
            "Task execution failed ( resubmitting infinite time until done): " + sch.toString)
          enQueue(sch, proxies)
          log.info("Requeued 1 message. Queue size: " + queuedTasks.keys.size)
        } else {
          proxies.foreach(_.actor ! TaskFailedMessageToProxy(sch, cause))
          log.error(cause, "Task execution failed: " + sch.toString)
        }
    }

  private def launcherCrashed(crashedLauncher: LauncherActor): Unit = {
    val msgs =
      scheduledMessages.toSeq.filter(_._2._1 === crashedLauncher).map(_._1)
    msgs.foreach { (sch: ScheduleTask) =>
      val proxies = scheduledMessages(sch)._3
      scheduledMessages.remove(sch)
      enQueue(sch, proxies)
    }
    log.info(
      "Requeued " + msgs.size + " messages. Queue size: " + queuedTasks.keys.size)

    knownLaunchers -= crashedLauncher
  }

  override def preStart: Unit = {
    log.debug("TaskQueue starting.")
  }

  override def postStop: Unit = {
    log.info("TaskQueue stopped.")
  }

  def addProxiesToScheduledMessages(sch: ScheduleTask,
                                    newproxies: List[Proxy]): Unit = {
    val (launcher, allocation, proxies) = scheduledMessages(sch)
    scheduledMessages
      .update(sch, (launcher, allocation, (newproxies ::: proxies).distinct))
  }

  def receive = {
    case sch: ScheduleTask =>
      log.debug("Received ScheduleTask.")
      val proxy = Proxy(sender)
      val queuedButSentByADifferentProxy = (queuedTasks.contains(sch) && (!queuedTasks(
        sch).has(proxy)))
      val scheduledButSentByADifferentProxy = scheduledMessages
        .get(sch)
        .map {
          case (_, _, proxies) =>
            !proxies.isEmpty && !proxies.contains(proxy)
        }
        .getOrElse(false)

      if (queuedButSentByADifferentProxy) {
        enQueue(sch, proxy :: Nil)
      } else if (scheduledButSentByADifferentProxy) {
        log.debug(
          "Scheduletask received multiple times from different proxies. Not queueing this one, but delivering result if ready. {}",
          sch)
        addProxiesToScheduledMessages(sch, proxy :: Nil)
      } else {
        sch.cacheActor ! CheckResult(sch, proxy)
      }

    case AnswerFromCache(message, proxy, sch) =>
      log.debug("Cache answered.")
      message match {
        case Right(Some(result)) => {
          log.debug("Replying with a Result found in cache.")
          proxy.actor ! MessageFromTask(result)
        }
        case Right(None) => {
          log.debug("Task is not found in cache. Enqueue. ")
          enQueue(sch, proxy :: Nil)
        }
        case Left(_) => {
          log.debug("Task is not found in cache. Enqueue. ")
          enQueue(sch, proxy :: Nil)
        }

      }

    case AskForWork(availableResource) =>
      if (negotiation.isEmpty) {
        log.debug(
          "AskForWork. Sender: {}. Resource: {}. Negotition state: {}. Queue state: {}",
          sender,
          availableResource,
          negotiation,
          queuedTasks
            .map(x => (x._1.description.taskId, x._1.resource))
            .toSeq
        )

        val launcher = LauncherActor(sender)

        queuedTasks
          .find {
            case (sch, _) => availableResource.canFulfillRequest(sch.resource)
          }
          .foreach {
            case (sch, proxies) =>
              negotiation = Some(launcher -> sch)
              log.debug("Dequeued. Sending task to " + launcher)
              log.debug(negotiation.toString)

              if (!knownLaunchers.contains(launcher)) {
                knownLaunchers += launcher
                HeartBeatActor.watch(launcher.actor,
                                     LauncherStopped(launcher),
                                     self)
              }

              launcher.actor ! ScheduleWithProxy(sch, proxies)

          }
      } else {
        log.debug("AskForWork received but currently in negotiation state.")
      }

    case Ack(allocated) if negotiatingWithCurrentSender =>
      val sch = negotiation.get._2
      negotiation = None

      if (scheduledMessages.contains(sch)) {
        log.error(
          "Routed messages already contains task. This is unexpected and can lead to lost messages.")
      }

      val proxies = queuedTasks(sch)
      queuedTasks.remove(sch)

      scheduledMessages += ((sch, (LauncherActor(sender), allocated, proxies)))

    case TaskDone(sch, result) =>
      log.debug("TaskDone {} {}", sch, result)
      taskDone(sch, result)

    case TaskFailedMessageToQueue(sch, cause) => taskFailed(sch, cause)
    case LauncherStopped(launcher) =>
      log.info(s"LauncherStopped: $launcher")
      launcherCrashed(launcher)

    case Ping =>
      sender ! Pong

    case HowLoadedAreYou =>
      val qs = QueueStat(
        queuedTasks.toList
          .map(_._1)
          .map(x => (x.description.taskId.toString, x.resource))
          .toList,
        scheduledMessages.toSeq
          .map(x => x._1.description.taskId.toString -> x._2._2)
          .toList
      )
      context.system.eventStream.publish(qs)
      sender ! qs

    case other => log.warning("Unhandled message. " + other.toString)
  }
}
