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

import akka.actor.{Actor, PoisonPill, ActorRef, Cancellable, Props}
import akka.remote.DeadlineFailureDetector
import akka.remote.FailureDetector.Clock
import akka.remote.DisassociatedEvent

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit.{SECONDS}
import java.util.concurrent.{TimeUnit, ScheduledFuture}

import scala.collection.immutable.Seq
import scala.collection.mutable.Queue

import tasks.util.eq._
import tasks.shared._
import tasks.shared.monitor._
import tasks.fileservice._
import tasks.caching._
import tasks.util._
import tasks._

import upickle.Js

class TaskQueue extends Actor with akka.actor.ActorLogging {

  // ActorRef here is the proxy of the task
  private val queuedTasks =
    collection.mutable.Map[ScheduleTask, List[ActorRef]]()

  // Map(task -> (launcher,allocated,list of proxies))
  private val routedMessages = scala.collection.mutable
    .Map[ScheduleTask, (ActorRef, CPUMemoryAllocated, List[ActorRef])]()

  // This is non empty while waiting for response from the tasklauncher
  // during that, no other tasks are started
  private var negotiation: Option[(ActorRef, ScheduleTask)] = None

  private val knownLaunchers = scala.collection.mutable.HashSet[ActorRef]()

  private def enQueue(sch: ScheduleTask, ac: List[ActorRef]) {
    if (!routedMessages.contains(sch)) {
      queuedTasks.get(sch) match {
        case None => queuedTasks.update(sch, ac)
        case Some(acs) => queuedTasks.update(sch, (ac ::: acs).distinct)
      }
    } else addProxyToRoutedMessages(sch, ac)
  }

  private def removeFromRoutedMessages(sch: ScheduleTask) {
    // Remove from the list of sent (running) messages
    routedMessages.remove(sch)
  }

  private def taskDone(sch: ScheduleTask, r: UntypedResult) {
    // Remove from the list of sent (running) messages and notify proxies
    routedMessages.get(sch).foreach {
      case (_, _, proxies) =>
        proxies.foreach(_ ! MessageFromTask(r))
    }
    removeFromRoutedMessages(sch)

    if (queuedTasks.contains(sch)) {
      log.error("Should not be queued. {}", queuedTasks(sch))
    }
  }

  private def taskFailed(sch: ScheduleTask, cause: Throwable) {
    // Remove from the list of sent (running) messages
    routedMessages.get(sch).foreach {
      case (launcher, allocation, proxies) =>
        routedMessages.remove(sch)
        if (config.global.resubmitFailedTask) {
          log.error(
              cause,
              "Task execution failed ( resubmitting infinite time until done): " + sch.toString)
          enQueue(sch, proxies)
          log.info("Requeued 1 message. Queue size: " + queuedTasks.keys.size)
        } else {
          proxies.foreach { ac =>
            ac ! TaskFailedMessageToProxy(sch, cause)
          }
          log.error(cause, "Task execution failed: " + sch.toString)
        }
    }

  }

  private def launcherCrashed(crashedLauncher: ActorRef) {
    // put back the jobs into the queue
    val msgs =
      routedMessages.toSeq.filter(_._2._1 === crashedLauncher).map(_._1)
    msgs.foreach { (sch: ScheduleTask) =>
      val proxies = routedMessages(sch)._3
      routedMessages.remove(sch)
      enQueue(sch, proxies)
    }
    log.info(
        "Requeued " + msgs.size + " messages. Queue size: " + queuedTasks.keys.size)

    knownLaunchers -= crashedLauncher
  }

  override def preStart = {
    log.debug("TaskQueue starting.")
  }

  override def postStop {
    log.info("TaskQueue stopped.")
  }

  def addProxyToRoutedMessages(m: ScheduleTask,
                               newproxies: List[ActorRef]): Unit = {
    val (launcher, allocation, proxies) = routedMessages(m)
    routedMessages
      .update(m, (launcher, allocation, (newproxies ::: proxies).distinct))
  }

  def receive = {
    case m: ScheduleTask => {
      log.debug("Received ScheduleTask.")
      if ((queuedTasks.contains(m) && (!queuedTasks(m).has(sender)))) {
        enQueue(m, sender :: Nil)
      } else if (routedMessages
                   .get(m)
                   .map {
                     case (_, _, proxies) =>
                       !proxies.isEmpty && !proxies.contains(sender)
                   }
                   .getOrElse(false)) {
        log.debug(
            "Scheduletask received multiple times from different proxies. Not queueing this one, but delivering result if ready. {}",
            m)
        addProxyToRoutedMessages(m, sender :: Nil)
      } else {
        val ch = sender
        m.cacheActor ! CheckResult(m, ch)
      }
    }
    case AnswerFromCache(message, ch, sch) => {
      val m = sch
      log.debug("Cache answered.")
      message match {
        case Right(Some(r)) => {
          log.debug("Replying with a Result found in cache.")
          ch ! (MessageFromTask(r))
        }
        case Right(None) => {
          log.debug("Task is not found in cache. Enqueue. ")
          enQueue(m, ch :: Nil)
          ch ! ATaskWasForwarded
        }
        case Left(_) => {
          log.debug("Task is not found in cache. Enqueue. ")
          enQueue(m, ch :: Nil)
          ch ! ATaskWasForwarded
        }

      }
    }
    // case NegotiationTimeout =>
    //   log.debug("TaskLauncher did not Ack'd back on sending task. Requeueing.")
    //   negotiation = None

    case AskForWork(resource) =>
      if (negotiation.isEmpty) {
        log.debug(
            "AskForWork. Sender: {}. Resource: {}. Negotition state: {}. Queue state: {}",
            sender,
            resource,
            negotiation,
            queuedTasks
              .map(x => (x._1.description.taskId, x._1.resource))
              .toSeq)

        val launcher = sender

        queuedTasks.find {
          case (k, v) => resource.canFulfillRequest(k.resource)
        }.foreach { task =>
          negotiation = Some(launcher -> task._1)
          log.debug("Dequeued. Sending task to " + launcher)
          log.debug(negotiation.toString)

          if (!knownLaunchers.contains(launcher)) {
            knownLaunchers += launcher
            context.actorOf(Props(new HeartBeatActor(launcher))
                              .withDispatcher("heartbeat"),
                            "heartbeatOf" + launcher.path.address.toString
                              .replace("://", "___") + launcher.path.name)
            context.system.eventStream
              .subscribe(self, classOf[HeartBeatStopped])
          }

          val resp = launcher ! ScheduleWithProxy(task._1, task._2)

        // context.system.scheduler.scheduleOnce(
        //     30 seconds,
        //     self,
        //     NegotiationTimeout)(context.dispatcher)

        }
      } else {
        log.debug("AskForWork received but currently in negotiation state.")
      }

    case Ack(allocated)
        if negotiation.map(_._1 === sender).getOrElse(false) => {
      val task = negotiation.get._2
      negotiation = None

      if (routedMessages.contains(task)) {
        log.error(
            "Routed messages already contains task. This is unexpected and can lead to lost messages.")
      }

      val proxies = queuedTasks(task)
      queuedTasks.remove(task)

      routedMessages += (task -> (sender, allocated, proxies))
    }

    case TaskDone(sch, result) =>
      log.debug("TaskDone {} {}", sch, result)
      taskDone(sch, result)

    case TaskFailedMessageToQueue(sch, cause) => taskFailed(sch, cause)
    case m: HeartBeatStopped => {
      log.info("HeartBeatStopped: " + m)

      launcherCrashed(m.ac)

    }
    case Ping => {
      sender ! true
      sender ! Pong
    }

    case HowLoadedAreYou => {
      // EventHandler.debug(this,queue.toString+routedMessages.toString)
      val qs = QueueStat(
          queuedTasks.toList
            .map(_._1)
            .map(x => (x.description.taskId.toString, x.resource))
            .toList,
          routedMessages.toSeq
            .map(x => x._1.description.taskId.toString -> x._2._2)
            .toList)
      context.system.eventStream.publish(qs)
      sender ! qs
    }

    case GetQueueInformation => sender ! QueueInfo(queuedTasks.toMap)

    case m => log.warning("Unhandled message. " + m.toString)
  }
}
