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

import akka.actor.{
  Actor,
  PoisonPill,
  ActorRef,
  Props,
  Cancellable,
  ExtendedActorSystem
}

import scala.concurrent.duration._
import scala.util.{Failure, Success}

import tasks.util._
import tasks.util.config._
import tasks.shared._
import tasks.fileservice._
import tasks.wire._

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import tasks.caching.TaskResultCache
import akka.util.Timeout

object Base64DataHelpers {
  def toBytes(b64: Base64Data): Array[Byte] = base64(b64.value)
  def apply(b: Array[Byte]): Base64Data = Base64Data(base64(b))
}

case class ScheduleTask(
    description: HashedTaskDescription,
    inputDeserializer: Spore[AnyRef, AnyRef],
    outputSerializer: Spore[AnyRef, AnyRef],
    function: Spore[AnyRef, AnyRef],
    resource: VersionedResourceRequest,
    queueActor: ActorRef,
    fileServicePrefix: FileServicePrefix,
    tryCache: Boolean,
    priority: Priority,
    labels: Labels,
    lineage: TaskLineage,
    proxy: ActorRef
)

object ScheduleTask {
  implicit def codec(as: ExtendedActorSystem): JsonValueCodec[ScheduleTask] = {
    implicit val actorRefCod = tasks.wire.actorRefCodec(as)
    val _ = (as, actorRefCod)
    JsonCodecMaker.make
  }

}

object Launcher {
  private[tasks] case class InternalMessageFromTask(
      actor: Task,
      result: UntypedResultWithMetadata
  )

  private[tasks] case class InternalMessageTaskFailed(
      actor: Task,
      cause: Throwable
  )

  private[tasks] case class Release(task: Task)
}

class Launcher(
    queueActor: ActorRef,
    nodeLocalCache: NodeLocalCache.State,
    slots: VersionedResourceAvailable,
    refreshInterval: FiniteDuration,
    remoteStorage: RemoteFileStorage,
    managedStorage: ManagedFileStorage,
    cache: TaskResultCache
)(implicit config: TasksConfig)
    extends Actor
    with akka.actor.ActorLogging {

  private case object CheckQueue
  private case object PrintResources

  private val maxResources: VersionedResourceAvailable = slots
  private var availableResources: VersionedResourceAvailable = maxResources

  private def isIdle = runningTasks.isEmpty
  private var idleState: Long = 0L

  private var denyWorkBeforeShutdown = false

  private var runningTasks
      : List[(Task, ScheduleTask, VersionedResourceAllocated, Long)] =
    Nil

  private var resourceDeallocatedAt: Map[Task, Long] = Map()

  private var freed = Set[Task]()

  private def launch(scheduleTask: ScheduleTask) = {

    log.debug("Launch method")

    val allocatedResource = availableResources.maximum(scheduleTask.resource)
    availableResources = availableResources.substract(allocatedResource)

    val filePrefix =
      if (config.createFilePrefixForTaskId)
        scheduleTask.fileServicePrefix.append(
          scheduleTask.description.taskId.id
        )
      else scheduleTask.fileServicePrefix

    import scala.reflect.runtime.{universe => ru}

    val task: Task =
      ru.runtimeMirror(getClass().getClassLoader())
        .reflectClass(ru.typeOf[Task].typeSymbol.asClass)
        .reflectConstructor(
          ru.typeOf[Task].decl(ru.termNames.CONSTRUCTOR).asMethod
        )(
          scheduleTask.inputDeserializer,
          scheduleTask.outputSerializer,
          scheduleTask.function,
          self,
          scheduleTask.queueActor,
          FileServiceComponent(
            managedStorage,
            remoteStorage
          ),
          cache,
          nodeLocalCache,
          allocatedResource.cpuMemoryAllocated,
          filePrefix,
          config,
          scheduleTask.priority,
          scheduleTask.labels,
          scheduleTask.description.taskId,
          scheduleTask.lineage.inherit(scheduleTask.description),
          scheduleTask.description,
          scheduleTask.proxy,
          context.system
        )
        .asInstanceOf[Task]
    import akka.pattern.ask
    import context.dispatcher
    (scheduleTask.proxy
      .?(NeedInput)(Timeout(2147483.seconds)))
      .mapTo[InputData]
      .foreach { input =>
        task.start(input)
        log.debug("Task started")
      }

    runningTasks = (
      task,
      scheduleTask,
      allocatedResource,
      System.nanoTime
    ) :: runningTasks

    allocatedResource
  }

  private def askForWork(): Unit = {
    if (!availableResources.empty && !denyWorkBeforeShutdown) {
      queueActor ! AskForWork(availableResources)
    }
  }

  private var scheduler: Cancellable = null
  private var logScheduler: Cancellable = null

  override def preStart(): Unit = {
    log.debug("TaskLauncher starting")

    import context.dispatcher

    logScheduler = context.system.scheduler.scheduleAtFixedRate(
      initialDelay = 0 seconds,
      interval = 20 seconds,
      receiver = self,
      message = PrintResources
    )
    scheduler = context.system.scheduler.scheduleAtFixedRate(
      initialDelay = 0 seconds,
      interval = refreshInterval,
      receiver = self,
      message = CheckQueue
    )

  }

  override def postStop(): Unit = {
    scheduler.cancel()

    logScheduler.cancel()

    log.info(
      s"TaskLauncher stopped, sent PoisonPill to ${runningTasks.size} running tasks."
    )
  }

  private def taskFinished(
      taskActor: Task,
      receivedResult: UntypedResultWithMetadata
  ): Unit = {
    val elem = runningTasks
      .find(_._1 == taskActor)
      .getOrElse(
        throw new RuntimeException("Wrong message received. No such taskActor.")
      )
    val scheduleTask = elem._2
    val resourceAllocated = elem._3
    val elapsedTime = ElapsedTimeNanoSeconds(
      resourceDeallocatedAt.get(taskActor).getOrElse(System.nanoTime) - elem._4
    )

    if (!receivedResult.noCache) {
      import context.dispatcher
      import cats.effect.unsafe.implicits.global

      cache
        .saveResult(
          scheduleTask.description,
          receivedResult.untypedResult,
          scheduleTask.fileServicePrefix
            .append(scheduleTask.description.taskId.id)
        )
        .timeout(config.cacheTimeout)
        .attempt
        .unsafeToFuture()
        .onComplete {
          case Failure(e) =>
            log.error(e, s"Failed to save ${scheduleTask.description}")
          case Success(_) =>
            queueActor ! TaskDone(
              scheduleTask,
              receivedResult,
              elapsedTime,
              resourceAllocated.cpuMemoryAllocated
            )
        }
    } else {
      queueActor ! TaskDone(
        scheduleTask,
        receivedResult,
        elapsedTime,
        resourceAllocated.cpuMemoryAllocated
      )
    }

    runningTasks = runningTasks.filterNot(_ == elem)
    if (!freed.contains(taskActor)) {
      availableResources = availableResources.addBack(resourceAllocated)
    } else {
      freed -= taskActor
      resourceDeallocatedAt -= taskActor
    }
  }

  private def taskFailed(taskActor: Task, cause: Throwable): Unit = {

    val elem = runningTasks
      .find(_._1 == taskActor)
      .getOrElse(
        throw new RuntimeException("Wrong message received. No such taskActor.")
      )
    val sch = elem._2

    runningTasks = runningTasks.filterNot(_ == elem)

    if (!freed.contains(taskActor)) {
      availableResources = availableResources.addBack(elem._3)
    } else {
      freed -= taskActor
      resourceDeallocatedAt -= taskActor
    }

    queueActor ! TaskFailedMessageToQueue(sch, cause)

  }

  def receive = {
    case Schedule(scheduleTask) =>
      log.debug(s"Received ScheduleWithProxy ")
      if (!denyWorkBeforeShutdown) {

        if (isIdle) {
          idleState += 1
        }
        val allocated = launch(scheduleTask)
        sender() ! QueueAck(allocated)
        askForWork()
      }

    case Launcher.InternalMessageFromTask(actor, result) =>
      taskFinished(actor, result)
      askForWork()

    case Launcher.InternalMessageTaskFailed(actor, cause) =>
      taskFailed(actor, cause)
      askForWork()

    case PrintResources =>
      log.info(s"Available resources: $availableResources on $self")
    case CheckQueue => askForWork()
    case Ping       => sender() ! Pong
    case PrepareForShutdown =>
      if (isIdle) {
        denyWorkBeforeShutdown = true
        sender() ! ReadyForShutdown
      }

    case WhatAreYouDoing =>
      val idle = isIdle
      log.debug(s"Received WhatAreYouDoing. idle:$idle, idleState:$idleState")
      if (idle) {
        sender() ! Idling(idleState)
      } else {
        sender() ! Working
      }

    case Launcher.Release(taskActor) =>
      val allocated = runningTasks.find(_._1 == taskActor).map(_._3)
      if (allocated.isEmpty) log.error("Can't find actor ")
      else {
        availableResources = availableResources.addBack(allocated.get)
        freed = freed + taskActor
        resourceDeallocatedAt = resourceDeallocatedAt + (
          (
            taskActor,
            System.nanoTime
          )
        )
      }
      askForWork()

    case other => log.debug("unhandled" + other)

  }

}
