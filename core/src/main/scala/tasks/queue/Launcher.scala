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

import scala.concurrent.duration._
import scala.util.{Failure, Success}

import tasks.util._
import tasks.util.config._
import tasks.shared._
import tasks.fileservice._
import tasks.wire._
import tasks.util.message._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import tasks.caching.TaskResultCache
import cats.effect.unsafe.implicits.global
import cats.effect.kernel.Ref
import tasks.util.Actor.ActorBehavior
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.FiberIO
import tasks.util.message.MessageData.NothingForSchedule

private[tasks] object Base64DataHelpers {
  def toBytes(b64: Base64Data): Array[Byte] = base64(b64.value)
  def apply(b: Array[Byte]): Base64Data = Base64Data(base64(b))
}

private[tasks] object Launcher {

  def makeHandle(
      queue: Queue,
      nodeLocalCache: NodeLocalCache.State,
      slots: VersionedResourceAvailable,
      refreshInterval: FiniteDuration,
      remoteStorage: RemoteFileStorage,
      managedStorage: ManagedFileStorage,
      cache: TaskResultCache,
      messenger: Messenger,
      address: Address
  )(implicit config: TasksConfig) = tasks.util.Actor.makeFromBehavior(
    new LauncherBehavior(
      queue,
      nodeLocalCache,
      slots,
      refreshInterval,
      remoteStorage,
      managedStorage,
      cache,
      messenger,
      address
    ),
    messenger
  )

  case class State(
      maxResources: VersionedResourceAvailable,
      availableResources: VersionedResourceAvailable,
      idleState: Long = 0,
      denyWorkBeforeShutdown: Boolean = false,
      waitingForWork: Boolean = false,
      runningTasks: List[
        (Task, MessageData.ScheduleTask, VersionedResourceAllocated, Long)
      ] = Nil,
      resourceDeallocatedAt: Map[Task, Long] = Map(),
      freed: Set[Task] = Set[Task](),
      fibers: List[FiberIO[Unit]] = Nil
  ) {
    def isIdle = runningTasks.isEmpty
  }

  case class LauncherActor(address: tasks.util.message.Address)

  final class LauncherHandle(
      address: tasks.util.message.Address,
      ref: Ref[IO, Launcher.State],
      queue: Queue,
      cache: TaskResultCache,
      messenger: Messenger,
      config: TasksConfig,
      nodeLocalCache: NodeLocalCache.State,
      remoteStorage: RemoteFileStorage,
      managedStorage: ManagedFileStorage
  ) { handle =>

    def askForWork(
        ref: Ref[IO, State],
        messenger: Messenger,
        address: Address,
        queue: Queue
    ): IO[Unit] = {
      def launch(
          state: State,
          scheduleTask: MessageData.ScheduleTask,
          ref: Ref[IO, State]
      ) = {

        scribe.debug("Launch method")

        val allocatedResource =
          state.availableResources.maximum(scheduleTask.resource)
        val newState = state.copy(availableResources =
          state.availableResources.substract(allocatedResource)
        )

        val filePrefix =
          if (config.createFilePrefixForTaskId)
            scheduleTask.fileServicePrefix.append(
              scheduleTask.description.taskId.id
            )
          else scheduleTask.fileServicePrefix

        val task: Task =
          new Task(
            scheduleTask.inputDeserializer,
            scheduleTask.outputSerializer,
            scheduleTask.function,
            handle,
            queue,
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
            messenger
          )

        val sideEffect =
          messenger.submit(
            Message(
              MessageData.NeedInput,
              from = address,
              to = scheduleTask.proxy
            )
          )
        val newState2 = newState.copy(
          runningTasks = (
            task,
            scheduleTask,
            allocatedResource,
            System.nanoTime
          ) :: newState.runningTasks
        )

        (allocatedResource, newState2, sideEffect)
      }

      ref.flatModify { state =>
        if (!state.denyWorkBeforeShutdown && !state.waitingForWork) {

          val effect: IO[Unit] = queue
            .askForWork(LauncherActor(address), state.availableResources)
            .flatMap {
              case Left(MessageData.NothingForSchedule) =>
                ref.flatModify { state =>
                  state.copy(waitingForWork = false) -> IO.unit
                }
              case Right(MessageData.Schedule(scheduleTask)) =>
                ref.flatModify { state =>
                  scribe.debug(s"Received ScheduleWithProxy ")
                  val st0 = state.copy(waitingForWork = false)
                  val (newState, sideEffects) =
                    if (!st0.denyWorkBeforeShutdown) {

                      val st1 = if (st0.isIdle) {
                        st0.copy(idleState = st0.idleState + 1)
                      } else st0
                      val (allocated, st2, io1) = launch(st1, scheduleTask, ref)
                      val io2 = queue.ack(allocated, address)
                      val io3 =
                        askForWork(ref, messenger, address, queue)
                      (st2, io1 *> io2 *> io3)
                    } else (st0, IO.unit)
                  newState -> sideEffects
                }
            }

          (state.copy(waitingForWork = true), effect)
        } else {
          scribe.debug(
            "Not asking for work because no available resources or preparing for shut down."
          )
          (state, IO.pure(Left(NothingForSchedule)))
        }
      }
    }

    private[tasks] val launcherActor = LauncherActor(address)
    def release(task: Task) = {
      ref.update { state =>
        val allocated = state.runningTasks.find(_._1 == task).map(_._3)
        val newState = if (allocated.isEmpty) {
          scribe.error("Can't find actor ")
          state
        } else {
          state.copy(
            availableResources =
              state.availableResources.addBack(allocated.get),
            freed = state.freed + task,
            resourceDeallocatedAt = state.resourceDeallocatedAt + (
              (
                task,
                System.nanoTime
              )
            )
          )
        }
        newState
      } *> askForWork(ref, messenger, address, queue)
    }

    private[tasks] def internalMessageFromTask(
        task: Task,
        result: UntypedResultWithMetadata
    ) = {
      ref.flatModify { state =>
        taskFinished(state, task, result)
      } *> askForWork(ref, messenger, address, queue)

    }
    private[tasks] def internalMessageTaskFailed(
        task: Task,
        cause: Throwable
    ) = {

      ref.flatModify { state =>
        taskFailed(state, task, cause)
      } *> askForWork(ref, messenger, address, queue)

    }

    private def taskFinished(
        state: Launcher.State,
        taskActor: Task,
        receivedResult: UntypedResultWithMetadata
    ) = {
      val elem = state.runningTasks
        .find(_._1 == taskActor)
        .getOrElse(
          throw new RuntimeException(
            "Wrong message received. No such taskActor."
          )
        )
      val scheduleTask = elem._2
      val resourceAllocated = elem._3
      val elapsedTime = ElapsedTimeNanoSeconds(
        state.resourceDeallocatedAt
          .get(taskActor)
          .getOrElse(System.nanoTime) - elem._4
      )

      val sideEffect = if (!receivedResult.noCache) {

        cache
          .saveResult(
            scheduleTask.description,
            receivedResult.untypedResult,
            scheduleTask.fileServicePrefix
              .append(scheduleTask.description.taskId.id)
          )
          .timeout(config.cacheTimeout)
          .attempt
          .flatMap {
            case Left(e) =>
              IO(scribe.error(e, s"Failed to save ${scheduleTask.description}"))
            case Right(_) =>
              queue.taskSuccess(
                scheduleTask,
                receivedResult,
                elapsedTime,
                resourceAllocated.cpuMemoryAllocated
              )

          }

      } else {
        queue.taskSuccess(
          scheduleTask,
          receivedResult,
          elapsedTime,
          resourceAllocated.cpuMemoryAllocated
        )

      }

      val st0 = state.copy(
        runningTasks = state.runningTasks.filterNot(_ == elem)
      )
      val st1 = if (!st0.freed.contains(taskActor)) {
        st0.copy(availableResources =
          st0.availableResources.addBack(resourceAllocated)
        )
      } else {
        st0.copy(
          freed = st0.freed - taskActor,
          resourceDeallocatedAt = st0.resourceDeallocatedAt - taskActor
        )
      }
      (st1, sideEffect)
    }

    private def taskFailed(
        state: Launcher.State,
        taskActor: Task,
        cause: Throwable
    ) = {

      val elem = state.runningTasks
        .find(_._1 == taskActor)
        .getOrElse(
          throw new RuntimeException(
            "Wrong message received. No such taskActor."
          )
        )
      val sch = elem._2

      val st0 =
        state.copy(runningTasks = state.runningTasks.filterNot(_ == elem))

      val st1 = if (!st0.freed.contains(taskActor)) {
        st0.copy(availableResources = st0.availableResources.addBack(elem._3))
      } else {
        st0.copy(
          freed = st0.freed - taskActor,
          resourceDeallocatedAt = st0.resourceDeallocatedAt - taskActor
        )
      }

      val sideEffect = queue.taskFailed(sch, cause)

      (st1, sideEffect)

    }
  }

  private[tasks] class LauncherBehavior(
      queue: Queue,
      nodeLocalCache: NodeLocalCache.State,
      slots: VersionedResourceAvailable,
      refreshInterval: FiniteDuration,
      remoteStorage: RemoteFileStorage,
      managedStorage: ManagedFileStorage,
      cache: TaskResultCache,
      messenger: Messenger,
      val address: Address
  )(implicit config: TasksConfig)
      extends ActorBehavior[Launcher.State, LauncherHandle](messenger) {
    override def release(st: Launcher.State): IO[Unit] =
      IO.parSequenceN(1)(st.fibers.map(_.cancel)).void
    def derive(
        ref: Ref[IO, Launcher.State]
    ): LauncherHandle =
      new LauncherHandle(
        address = address,
        ref = ref,
        queue = queue,
        cache = cache,
        messenger = messenger,
        config = config,
        nodeLocalCache = nodeLocalCache,
        remoteStorage = remoteStorage,
        managedStorage = managedStorage
      )

    val init: Launcher.State =
      Launcher.State(maxResources = slots, availableResources = slots)

    override def schedulers(
        ref: Ref[IO, Launcher.State]
    ): Option[IO[fs2.Stream[IO, Unit]]] = Some(
      IO {
        val scribeScheduler =
          fs2.Stream.fixedRate[IO](20 seconds).evalMap { _ =>
            ref.get.flatMap(state =>
              IO(
                scribe.info(
                  s"Available resources: ${state.availableResources} on $address"
                )
              )
            )
          }
        val askForWorkScheduler =
          fs2.Stream
            .fixedRate[IO](refreshInterval)
            .evalMap(_ =>
              derive(ref).askForWork(ref, messenger, address, queue)
            )

        scribeScheduler.mergeHaltBoth(askForWorkScheduler)
      }
    )

    def receive = (state, stateRef) => {
      case Message(t: MessageData.InputData, from, _) =>
        state -> state.runningTasks
          .find(_._1.proxy == from)
          .get
          ._1
          .start(t)
          .start
          .flatMap { fiber =>
            stateRef.update(state => state.copy(fibers = fiber :: state.fibers))
          }

      case Message(MessageData.Ping, from, _) =>
        state -> sendTo(from, MessageData.Ping)

      case Message(MessageData.PrepareForShutdown, from, _) =>
        val (newState, sideEffect) = if (state.isIdle) {

          (
            state.copy(denyWorkBeforeShutdown = true),
            sendTo(from, MessageData.ReadyForShutdown)
          )
        } else (state, IO.unit)
        newState -> sideEffect

      case Message(MessageData.WhatAreYouDoing, from, _) =>
        val idle = state.isIdle
        scribe.debug(
          s"Received WhatAreYouDoing. idle:$idle, idleState:${state.idleState}"
        )
        if (idle) {
          state -> sendTo(from, MessageData.Idling(state.idleState))
        } else {
          state -> sendTo(from, MessageData.Working)
        }

      case other => state -> IO(scribe.debug("unhandled" + other))

    }

  }
}
