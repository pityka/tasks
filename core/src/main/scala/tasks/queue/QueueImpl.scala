package tasks.queue

import tasks.util.message.MessageData.ScheduleTask
import cats.effect.IO
import tasks.caching.TaskResultCache
import tasks.util.Messenger
import tasks.util.message.Message
import tasks.util.message.MessageData
import tasks.util.message.Address
import tasks.shared.ElapsedTimeNanoSeconds
import tasks.shared.ResourceAllocated
import tasks.util.config.TasksConfig
import tasks.queue.Launcher.LauncherActor
import tasks.shared.VersionedResourceAvailable
import tasks.util.HeartBeatIO
import tasks.shared.VersionedResourceAllocated
import cats.effect.kernel.Ref
import tasks.util.Transaction

object QueueImpl {
  def fromTransaction(
      transaction: Transaction[TaskQueue.State],
      cache: TaskResultCache,
      messenger: Messenger
  )(implicit config: TasksConfig) : IO[QueueImpl] = {
    val q = new QueueImpl(transaction, cache, messenger)
    q.start.map(_ => q)
  }

  def initRef(cache: TaskResultCache, messenger: Messenger)(implicit
      config: TasksConfig
  ) = Ref.of[IO, TaskQueue.State](TaskQueue.State.empty).map { ref =>
    new QueueImpl(Transaction.fromRef(ref), cache, messenger)
  }
}

class QueueImpl(
    ref: Transaction[TaskQueue.State],
    cache: TaskResultCache,
    messenger: Messenger
)(implicit config: TasksConfig) {
  import TaskQueue._

  private def start = ref.get.flatMap { state =>
    IO.parSequenceN(16)(state.knownLaunchers.toList.map { launcher =>
      scribe.info(s"Found launcher in state. Start heartbeat on it $launcher")
      HeartBeatIO
        .make(
          target = launcher.address,
          sideEffect = handleLauncherStopped(launcher),
          messenger = messenger
        )
        .start
        .flatMap { heartBeatFiber =>
          ref.update(_.update(FiberCreated(heartBeatFiber)))

        }
    })
  }

  def release = ref.get.flatMap { st =>
    IO(
      scribe.debug(
        s"Releasing resources held by TasksQueue: ${st.fibers} fibers"
      )
    ) *> IO.parSequenceN(1)(st.fibers.map(_.cancel)).void
  }

  private def handleCacheAnwser(
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
                from = Address.unknown,
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

  def scheduleTask(sch: ScheduleTask) = ref.flatModify { state =>
    scribe.debug("Received ScheduleTask.")
    val proxy = Proxy(sch.proxy)

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
          .flatMap(r => handleCacheAnwser(r))
          .start
          .void
      } else {
        scribe.debug(
          "ScheduleTask should not be checked in the cache. Enqueue. "
        )
        state.update(Enqueued(sch, List(proxy))) -> IO.unit
      }

    }
  }

  private def handleLauncherStopped(
      launcher: LauncherActor
  ) = ref.update { state =>
    scribe.info(s"LauncherStopped: $launcher")
    import tasks.util.eq._
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

  def askForWork(
      launcher: LauncherActor,
      availableResource: VersionedResourceAvailable
  ): IO[Either[MessageData.NothingForSchedule.type, MessageData.Schedule]] =
    ref.flatModify { state =>
      if (state.negotiation.isEmpty) {
        scribe.debug(
          s"AskForWork $availableResource ${state.negotiation} ${state.queuedTasks.map { case (_, (sch, _)) =>
              (sch.description.taskId, sch.resource)
            }.toSeq}"
        )

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
            state -> IO.pure(Left(MessageData.NothingForSchedule))

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
                    sideEffect = handleLauncherStopped(launcher),
                    messenger = messenger
                  )
                  .start
                  .flatMap { heartBeatFiber =>
                    ref.update(_.update(FiberCreated(heartBeatFiber)))

                  }
              } else withNegotiation -> IO.unit

            val io2 = IO.pure(
              Right(MessageData.Schedule(sch))
            )

            newState -> (io1 *> io2)

        }

      } else {
        state -> IO.pure(Left(MessageData.NothingForSchedule))
      }
    }

  def ack(allocated: VersionedResourceAllocated, launcher: Address): IO[Unit] =
    ref.update { state =>
      if (state.negotiatingWithCurrentSender(launcher)) {
        val sch = state.negotiation.get._2
        if (state.scheduledTasks.contains(project(sch))) {
          scribe.error(
            "Routed messages already contains task. This is unexpected and can lead to lost messages."
          )
        }

        state
          .update(NegotiationDone)
          .update(TaskScheduled(sch, LauncherActor(launcher), allocated))
      } else state
    }

  def taskSuccess(
      sch: ScheduleTask,
      resultWithMetadata: UntypedResultWithMetadata,
      elapsedTime: ElapsedTimeNanoSeconds,
      resourceAllocated: ResourceAllocated
  ): IO[Unit] = ref.flatModify { state =>
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
                  from = Address.unknown,
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
  }

  def taskFailed(sch: ScheduleTask, cause: Throwable): IO[Unit] =
    ref.flatModify { state =>
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
                    from = Address.unknown,
                    to = pr.address
                  )
                )
              )
              scribe.error(cause, "Task execution failed: " + sch.toString)
              (removed, sideEffectAcc ++ sideEffects)
            }
        }
      updated -> IO.parSequenceN(1)(sideEffects).void
    }

  def queryLoad: IO[MessageData.QueueStat] = ref.get.map { state =>
    val qs = MessageData.QueueStat(
      state.queuedTasks.toList.map { case (_, (sch, _)) =>
        (sch.description.taskId.toString, sch.resource)
      }.toList,
      state.scheduledTasks.toSeq
        .map(x => x._1.description.taskId.toString -> x._2._2)
        .toList
    )

    qs

  }

}
