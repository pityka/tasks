/*
* The MIT License
*
* Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
* Group Fellay
* Copyright (c) 2016 Istvan Bartha
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

import akka.actor.{ Actor, PoisonPill, ActorRef, Props, Cancellable, ActorRefFactory }
import akka.actor.Actor._

import scala.concurrent.Future
import scala.concurrent.duration._

import java.lang.Class
import java.io.File
import java.util.concurrent.{ ScheduledFuture }

import tasks.util._
import tasks.shared.monitor._
import tasks.shared._
import tasks.fileservice._
import tasks.caching._
import tasks.elastic._

@SerialVersionUID(1L)
case class ScheduleWithProxy(sch: ScheduleTask, ac: List[ActorRef]) extends Serializable

@SerialVersionUID(1L)
case class TaskDescription(taskID: String, startData: Prerequisitive[_]) extends Ordered[TaskDescription] with Serializable {
  def compare(that: TaskDescription) = this.hashCode - that.hashCode

  override val hashCode = 41 + (41 * (taskID.hashCode + 41 * startData.hashCode))

  def persistent = this.copy(startData = startData.persistent)

}

@SerialVersionUID(1L)
case class ScheduleTask(
    description: TaskDescription,
    taskImplementation: String,
    resource: CPUMemoryRequest,
    balancerActor: ActorRef,
    fileServiceActor: ActorRef,
    fileServicePrefix: FileServicePrefix,
    cacheActor: ActorRef
) extends Serializable {

  def startData = description.startData
}

class TaskLauncher(
    taskQueue: ActorRef,
    nodeLocalCache: ActorRef,
    slots: CPUMemoryAvailable = CPUMemoryAvailable(cpu = 1, memory = 2000),
    refreshRate: FiniteDuration = 100 milliseconds,
    hostsForMPI: Seq[HostForMPI] = Nil
) extends Actor with akka.actor.ActorLogging {

  private[this] val maxResources: CPUMemoryAvailable = slots
  private[this] var availableResources: CPUMemoryAvailable = maxResources

  private def idle = maxResources == availableResources
  private var idleState: Long = 0L

  private var denyWorkBeforeShutdown = false

  private[this] var startedTasks: List[(ActorRef, ScheduleTask, CPUMemoryAllocated)] = Nil

  private def launch(sch: ScheduleTask, proxies: List[ActorRef]) = {

    log.debug("Launch method")

    val allocatedResource = availableResources.maximum(sch.resource)
    availableResources = availableResources.substract(allocatedResource)

    // if all cpu have been allocated for this job then allow use of MPI
    // reason is that the hostsForMPI resources are not properly tracked so
    // one job has to use all of them at once
    val hostsForMPIEdited = if (availableResources.cpu == 0) hostsForMPI else Nil

    val actor = context.actorOf(
      Props(
        classOf[Task[_, _]],
        Class.forName(sch.taskImplementation).asInstanceOf[java.lang.Class[_]].getConstructor().newInstance(),
        self,
        sch.balancerActor,
        sch.fileServiceActor,
        sch.cacheActor,
        nodeLocalCache,
        allocatedResource,
        hostsForMPIEdited,
        sch.fileServicePrefix.append(sch.description.taskID)
      ).withDispatcher("task-worker-blocker-dispatcher")
    )
    log.debug("Actor constructed")
    // log.debug( "Actor started")
    proxies.foreach { sender =>
      actor ! RegisterForNotification(sender)
    }

    startedTasks = (actor, sch, allocatedResource) :: startedTasks

    log.debug("Sending startdata to actor.")
    actor ! sch.startData
    log.debug("startdata sent.")
    allocatedResource
  }

  private def askForWork {
    if (!availableResources.empty && !denyWorkBeforeShutdown) {
      // log.debug("send askForWork" + availableResources)
      taskQueue ! AskForWork(availableResources)
    }
  }

  private var scheduler: Cancellable = null

  override def preStart {
    log.debug("TaskLauncher starting")

    import context.dispatcher

    scheduler = context.system.scheduler.schedule(
      initialDelay = 0 seconds,
      interval = refreshRate,
      receiver = self,
      message = CheckQueue
    )

  }

  override def postStop {
    scheduler.cancel

    startedTasks.foreach(x => x._1 ! PoisonPill)
    log.info("TaskLauncher stopped, sent PoisonPill to ${startedTasks.size} running tasks.")
  }

  private def taskFinished(taskActor: ActorRef, receivedResult: Result) {
    val elem = startedTasks.find(_._1 == taskActor).getOrElse(throw new RuntimeException("Wrong message received. No such taskActor."))
    val sch: ScheduleTask = elem._2

    sch.cacheActor.!(SaveResult(sch, receivedResult))(sender = taskActor)

    taskQueue ! TaskDone(sch, receivedResult)

    startedTasks = startedTasks.filterNot(_ == elem)
    availableResources = availableResources.addBack(elem._3)
  }

  private def taskFailed(taskActor: ActorRef, cause: Throwable) {

    val elem = startedTasks.find(_._1 == taskActor).getOrElse(throw new RuntimeException("Wrong message received. No such taskActor."))
    val sch = elem._2

    startedTasks = startedTasks.filterNot(_ == elem)

    availableResources = availableResources.addBack(elem._3)

    taskQueue ! TaskFailedMessageToQueue(sch, cause)

  }

  def receive = {
    case ScheduleWithProxy(sch, acs) => {
      log.debug(s"Received ScheduleWithProxy from $acs")
      if (!denyWorkBeforeShutdown) {

        if (idle) {
          idleState += 1
        }
        val allocated = launch(sch, acs)
        sender ! Ack(allocated)
        askForWork
      }
    }
    case InternalMessageFromTask(actor, result) => {
      taskFinished(actor, result);
      askForWork
    }
    case InternalMessageTaskFailed(actor, cause) => {
      taskFailed(actor, cause)
      askForWork
    }
    case CheckQueue => askForWork
    case Ping => sender ! Pong
    case PrepareForShutdown => {
      if (idle) {
        denyWorkBeforeShutdown = true
        sender ! ReadyForShutdown
      }
    }
    case WhatAreYouDoing => {
      log.debug(s"Received WhatAreYouDoing. idle:$idle, idleState:$idleState")
      if (idle) {
        sender ! Idling(idleState)
      } else {
        sender ! Working
      }
    }
    case BlockOn(request) => {
      availableResources = availableResources.addBack(CPUMemoryAllocated(request.cpu._1, request.memory))
    }
    case BlockOff(request) => {
      availableResources = availableResources.substract(CPUMemoryAllocated(request.cpu._1, request.memory))
    }

    case x => log.debug("unhandled" + x)

  }

}
