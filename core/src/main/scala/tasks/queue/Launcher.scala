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
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.FiberIO
import tasks.util.message.MessageData.NothingForSchedule
import cats.effect.kernel.Deferred
import cats.effect.ExitCode
import tasks.elastic.ShutdownSelfNode
import cats.effect.kernel.Resource
import cats.syntax.all

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
      address: LauncherName,
      node: Option[Node],
      shutdown: Option[tasks.elastic.ShutdownSelfNode],
      exitCode: Option[Deferred[IO, ExitCode]]
  )(implicit config: TasksConfig) = {
    def release(st: Launcher.State): IO[Unit] =
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
        managedStorage = managedStorage,
        node = node,
        shutdown = shutdown,
        exitCode = exitCode
      )

    val init: IO[Launcher.State] =
      IO.pure(
        Launcher.State(
          maxResources = slots,
          availableResources = slots,
          lastTaskFinished = 0L
        )
      )

    def schedulers(
        ref: Ref[IO, Launcher.State]
    ): IO[fs2.Stream[IO, Unit]] =
      IO {
        val scribeScheduler =
          fs2.Stream.fixedRate[IO](20 seconds).evalMap { _ =>
            ref.get.flatMap(state =>
              IO(
                scribe.debug(
                  s"Available resources: ",
                  state.availableResources,
                  address
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

        val incrementStream =
          fs2.Stream
            .fixedRate[IO](config.launcherActorHeartBeatInterval)
            .evalMap(_ => queue.increment(address))

        scribeScheduler
          .mergeHaltBoth(askForWorkScheduler)
          .mergeHaltBoth(incrementStream)

      }

    Resource.eval(init.flatMap(s => Ref.of[IO, State](s))).flatMap { stateRef =>
      val schedulerStream =
        schedulers(stateRef)

      val streamFiber = schedulerStream.flatMap { case stream =>
        stream
          .onFinalize(IO(scribe.debug(s"Stream terminated", address)))
          .compile
          .drain
          .start
      }

      val releaseIO =
        IO(
          scribe.debug(
            s"Will cancel fibers",
            address
          )
        ) *> stateRef.get.flatMap(release).void *> IO(
          scribe.debug(s"Canceled fibers ", address)
        )

      Resource
        .make(streamFiber)(fiber =>
          releaseIO *> fiber.cancel *> IO(
            scribe.debug(s"Streams of actor canceled.", address)
          )
        )
        .map { _ =>
          derive(stateRef)
        }

    }
  }

  case class State(
      maxResources: VersionedResourceAvailable,
      availableResources: VersionedResourceAvailable,
      lastTaskFinished: Long,
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

  final class LauncherHandle(
      val address: LauncherName,
      ref: Ref[IO, Launcher.State],
      queue: Queue,
      cache: TaskResultCache,
      messenger: Messenger,
      config: TasksConfig,
      nodeLocalCache: NodeLocalCache.State,
      remoteStorage: RemoteFileStorage,
      managedStorage: ManagedFileStorage,
      node: Option[Node],
      shutdown: Option[tasks.elastic.ShutdownSelfNode],
      exitCode: Option[Deferred[IO, ExitCode]]
  ) { handle =>

    def askForWork(
        ref: Ref[IO, State],
        messenger: Messenger,
        address: LauncherName,
        queue: Queue
    ): IO[Unit] = {
      def launch(
          state: State,
          scheduleTask: MessageData.ScheduleTask,
          ref: Ref[IO, State]
      ) = {

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

        scribe.debug(
          s"Launch",
          scheduleTask,
          allocatedResource,
          filePrefix,
          address
        )

        val task: Task =
          new Task(
            inputDeserializer = scheduleTask.inputDeserializer,
            outputSerializer = scheduleTask.outputSerializer,
            function = scheduleTask.function,
            launcherActor = handle,
            queue = queue,
            fileServiceComponent = FileServiceComponent(
              managedStorage,
              remoteStorage
            ),
            cache = cache,
            nodeLocalCache = nodeLocalCache,
            resourceAllocated = allocatedResource.cpuMemoryAllocated,
            fileServicePrefix = filePrefix,
            tasksConfig = config,
            priority = scheduleTask.priority,
            labels = scheduleTask.labels,
            taskId = scheduleTask.description.taskId,
            lineage = scheduleTask.lineage.inherit(scheduleTask.description),
            taskHash = scheduleTask.description,
            proxy = scheduleTask.proxy,
            messenger = messenger
          )

        val sideEffect = task
          .start(scheduleTask.input)
          .attempt
          .map {
            case Right(unit) =>
              unit
            case Left(value) =>
              scribe.error("Unexpected failure during task execution.", value)
              ()
          }
          .start
          .flatMap { fiber =>
            ref.update(state => state.copy(fibers = fiber :: state.fibers))
          }

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

      ref.flatModifyFull { case (poll, state) =>
        if (!state.denyWorkBeforeShutdown && !state.waitingForWork) {

          val effect: IO[Unit] = poll(
            queue
              .askForWork(address, state.availableResources, node)
          )
            .onCancel(ref.update { state =>
              state.copy(waitingForWork = false)
            })
            .flatMap {
              case Left(throwable) =>
                IO(
                  scribe.error(
                    s"Queue returned error on askForWork. Handling exception.",
                    throwable
                  )
                ) *>
                  ref.update { state =>
                    state.copy(waitingForWork = false)
                  }
              case Right(Left(MessageData.NothingForSchedule)) =>
                ref.update { state =>
                  state.copy(waitingForWork = false)
                }
              case Right(Right(MessageData.Schedule(scheduleTask))) =>
                ref.flatModifyFull { case (poll, state) =>
                  scribe.debug(s"Received Schedule ", scheduleTask)
                  val st0 = state.copy(waitingForWork = false)
                  val (newState, sideEffects) =
                    if (!st0.denyWorkBeforeShutdown) {

                      val st1 = st0
                      val (allocated, st2, io1) = launch(st1, scheduleTask, ref)
                      val io2 = poll(queue.ack(allocated, address))
                      (st2, io1 *> io2)
                    } else (st0, IO.unit)
                  newState -> sideEffects
                }
            }

          (state.copy(waitingForWork = true), effect)
        } else if (!state.denyWorkBeforeShutdown) {
          if (
            state.isIdle && FiniteDuration(
              System.nanoTime - state.lastTaskFinished,
              scala.concurrent.duration.NANOSECONDS
            ) > config.idleNodeTimeout && node.isDefined && exitCode.isDefined && shutdown.isDefined
          )
            state.copy(denyWorkBeforeShutdown = true) -> shutdown.get
              .shutdownRunningNode(exitCode.get, node.get.name) *> IO.pure(
              Left(NothingForSchedule)
            )
          else {
            state -> IO.pure(Left(NothingForSchedule))
          }
        } else {
          scribe.debug(
            "Not asking for work because no available resources or preparing for shut down.",
            address
          )
          (state, IO.pure(Left(NothingForSchedule)))
        }
      }
    }

    def release(task: Task) = {
      ref.update { state =>
        val allocated = state.runningTasks.find(_._1 == task).map(_._3)
        val newState = if (allocated.isEmpty) {
          scribe.error("Can't find proxy ", task.proxy, address)
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
              IO(scribe.error(e, s"Failed to save", scheduleTask, address))
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
      val st2 = st1.copy(lastTaskFinished = System.nanoTime)
      (st2, sideEffect)
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

      val st2 = st1.copy(lastTaskFinished = System.nanoTime)
      val sideEffect = queue.taskFailed(sch, cause)

      (st2, sideEffect)

    }
  }

}
