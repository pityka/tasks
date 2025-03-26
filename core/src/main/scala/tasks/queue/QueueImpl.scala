package tasks.queue

import tasks.util.message.MessageData.ScheduleTask
import cats.effect.IO
import tasks.caching.TaskResultCache
import tasks.util.Messenger
import tasks.util.message.Node
import tasks.util.message.Message
import tasks.util.message.MessageData
import tasks.util.message.Address
import tasks.shared.ElapsedTimeNanoSeconds
import tasks.shared.ResourceAllocated
import tasks.util.config.TasksConfig
import tasks.util.message.LauncherName
import tasks.shared.VersionedResourceAvailable
import tasks.util.HeartBeatIO
import tasks.util.eq._
import tasks.shared.VersionedResourceAllocated
import cats.effect.kernel.Ref
import cats.effect.FiberIO
import tasks.util.Transaction
import cats.effect.kernel.Resource
import tasks.elastic.NodeRegistryState

import tasks.elastic.ShutdownNode
import tasks.elastic.DecideNewNode
import tasks.elastic.CreateNode
import tasks.shared.RunningJobId

object QueueImpl {

  case class ScheduleTaskEqualityProjection(
      description: HashedTaskDescription
  )

  sealed trait Event
  case class Enqueued(sch: ScheduleTask, proxies: List[Proxy]) extends Event
  case class Incremented(launcher: LauncherName) extends Event
  case class ProxyAddedToScheduledMessage(
      sch: ScheduleTask,
      proxies: List[Proxy]
  ) extends Event
  case class Negotiating(launcher: LauncherName, sch: ScheduleTask)
      extends Event
  case class LauncherJoined(launcher: LauncherName, node: Option[Node])
      extends Event
  case object NegotiationDone extends Event
  case class TaskScheduled(
      sch: ScheduleTask,
      launcher: LauncherName,
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
  case class LauncherCrashed(crashedLauncher: LauncherName) extends Event
  case class CacheHit(sch: ScheduleTask, result: UntypedResult) extends Event
  case class NodeEvent(ev: NodeRegistryState.Event) extends Event

  def project(sch: ScheduleTask) =
    ScheduleTaskEqualityProjection(sch.description)

  case class State(
      queuedTasks: Map[
        ScheduleTaskEqualityProjection,
        (ScheduleTask, List[Proxy])
      ],
      scheduledTasks: Map[
        ScheduleTaskEqualityProjection,
        (LauncherName, VersionedResourceAllocated, List[Proxy], ScheduleTask)
      ],
      knownLaunchers: Map[LauncherName, Option[Node]],
      /*This is non empty while waiting for response from the tasklauncher
       *during that, no other tasks are started*/
      negotiation: Option[(LauncherName, ScheduleTask)],
      counters: Map[LauncherName, Long],
      nodes: NodeRegistryState.State
  ) {

    def update(e: Event): State = {
      e match {
        case NodeEvent(ev) =>
          copy(nodes = nodes.update(ev))
        case Incremented(launcher) =>
          copy(counters = counters.get(launcher) match {
            case None        => counters.updated(launcher, 0L)
            case Some(value) => counters.updated(launcher, value + 1)
          })
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
        case LauncherJoined(launcher, node) =>
          copy(knownLaunchers = knownLaunchers + (launcher -> node))
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
          copy(
            knownLaunchers = knownLaunchers - launcher,
            counters = counters - launcher
          )
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

    def negotiatingWithCurrentSender(sender: LauncherName) =
      negotiation.map(_._1 === sender).getOrElse(false)
  }

  object State {
    def empty =
      State(Map(), Map(), Map(), None, Map(), NodeRegistryState.State.empty)

  }

  private[tasks] def fromTransaction(
      transaction: Transaction[QueueImpl.State],
      cache: TaskResultCache,
      messenger: Messenger,
      shutdownNode: Option[tasks.elastic.ShutdownNode],
      decideNewNode: Option[tasks.elastic.DecideNewNode],
      createNode: Option[tasks.elastic.CreateNode],
      unmanagedResource: tasks.shared.ResourceAvailable
  )(implicit config: TasksConfig): Resource[IO, QueueImpl] = {
    Resource.make(Ref.of[IO, List[FiberIO[Unit]]](Nil).flatMap { ref =>
      val q = new QueueImpl(
        ref = transaction,
        fiberList = ref,
        cache = cache,
        messenger = messenger,
        shutdownNode = shutdownNode,
        decideNewNode = decideNewNode,
        createNode = createNode,
        unmanagedResource = unmanagedResource
      )
      q.startCounterLoops.map(_ => q)
    })(_.release)
  }

  private[tasks] def initRef(
      cache: TaskResultCache,
      messenger: Messenger,
      shutdownNode: Option[tasks.elastic.ShutdownNode],
      decideNewNode: Option[tasks.elastic.DecideNewNode],
      createNode: Option[tasks.elastic.CreateNode],
      unmanagedResource: tasks.shared.ResourceAvailable
  )(implicit
      config: TasksConfig
  ) = Resource.make(Ref.of[IO, QueueImpl.State](QueueImpl.State.empty).flatMap {
    ref =>
      Ref.of[IO, List[FiberIO[Unit]]](Nil).flatMap { ref2 =>
        val q = new QueueImpl(
          ref = Transaction.fromRef(ref),
          fiberList = ref2,
          cache = cache,
          messenger = messenger,
          shutdownNode = shutdownNode,
          decideNewNode = decideNewNode,
          createNode = createNode,
          unmanagedResource = unmanagedResource
        )
        q.startCounterLoops.map(_ => q)
      }
  })(_.release)
}

private[tasks] class QueueImpl(
    ref: Transaction[QueueImpl.State],
    fiberList: Ref[IO, List[FiberIO[Unit]]],
    cache: TaskResultCache,
    messenger: Messenger,
    shutdownNode: Option[tasks.elastic.ShutdownNode],
    decideNewNode: Option[tasks.elastic.DecideNewNode],
    createNode: Option[tasks.elastic.CreateNode],
    unmanagedResource: tasks.shared.ResourceAvailable
)(implicit config: TasksConfig) {
  import QueueImpl._

  def initFailed(n: RunningJobId): IO[Unit] = {
    val handleFailureIO  = createNode match {
    case None => IO.unit
    case Some(value) =>
      value.convertRunningToPending(n).flatMap {
        case Some(pending) =>
          ref.update(_.update(NodeEvent(NodeRegistryState.InitFailed(pending))))
        case None =>
          IO.unit
      }
  }

  handleFailureIO *> handleQueueStatIO
}

  def increment(launcher: LauncherName): IO[Unit] =
    ref.update(_.update(Incremented(launcher)))

  def knownLaunchers = ref.get.map(_.knownLaunchers)

  private def startCounterLoops = {
    def loop: IO[Unit] = ref.get
      .map(_.knownLaunchers.keySet.toList)
      .flatMap { launcher =>
        IO.parSequenceN(1)(launcher.map { launcher =>
          IO(scribe.info(s"Query counter $launcher")) *>
            HeartBeatIO.Counter.sideEffectWhenTimeout(
              query = ref.get.map(_.counters.get(launcher).getOrElse(0L)),
              sideEffect = handleLauncherStopped(launcher)
            )
        })
      }
      .map(_ => ())
      .attempt
      .map {
        case Left(e) =>
          scribe.error(
            "Error reading counters and/or handling stopped launchers",
            e
          )
          ()
        case _ => ()
      }
      .flatMap(_ => loop)
    loop.start.flatMap { fiber => fiberList.update(list => fiber :: list) }
  }

  def release = {
    val stopFibers = fiberList.get.flatMap { fibers =>
      IO(
        scribe.debug(
          s"Releasing resources held by TasksQueue: ${fibers.size} fibers"
        )
      ) *> IO.parSequenceN(1)(fibers.map(_.cancel)).void
    }
    val stopNodes = shutdownNode match {
      case None => IO.unit
      case Some(shutdownNode) =>
        ref.flatModify { st =>
          val shutdown = IO
            .parSequenceN(1)(
              (st.nodes.running.map { case (node, _) =>
                scribe.info("Shutting down node " + node)
                shutdownNode.shutdownRunningNode(node)

              } ++
                st.nodes.pending.map { case (node, _) =>
                  shutdownNode.shutdownPendingNode(node)

                }).toList
            )
            .map((a: List[Unit]) => ())
          st.update(NodeEvent(NodeRegistryState.AllStop)) -> shutdown
        }
    }

    IO.both(stopFibers, stopNodes).void
  }

  private def handleCacheAnwser(
      a: tasks.caching.AnswerFromCache
  ): IO[Unit] = {
    val message = a.message
    val proxy = a.sender
    val sch = a.sch
    scribe.debug("Cache answered.")
    val cacheIO = message match {
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
    cacheIO *> handleQueueStatIO
  }

  def scheduleTask(sch: ScheduleTask): IO[Unit] = {
    val scheduleIO = ref.flatModify { state =>
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
    scheduleIO *> handleQueueStatIO
  }

  private def handleNewNode(node: Node, createNode: CreateNode): IO[Unit] = {
    val runningId = node.name
    createNode.convertRunningToPending(runningId).flatMap {
      case Some(convertedRunningId) =>
        ref.update { state =>
          state.update(
            NodeEvent(NodeRegistryState.NodeIsUp(node, convertedRunningId))
          )
        }
      case None =>
        scribe.warn(s"Failed to convert back to pending $runningId")
        IO.unit
    }
  }

  private val handleQueueStatIO =
    createNode
      .flatMap { createNode =>
        shutdownNode
          .flatMap { shn =>
            decideNewNode.map { dn => handleQueueStat(shn, dn, createNode) }
          }
      }
      .getOrElse(IO.unit)

  private def handleQueueStat(
      shutdownNode: ShutdownNode,
      decideNewNode: DecideNewNode,
      createNode: CreateNode
  ) =
    ref.flatModify { state =>
      val queueStat = tasks.util.message.QueueStat(
        state.queuedTasks.toList.map { case (_, (sch, _)) =>
          (sch.description.taskId.toString, sch.resource)
        }.toList,
        state.scheduledTasks.toSeq
          .map(x => x._1.description.taskId.toString -> x._2._2)
          .toList
      )
      val logIO = if (config.logQueueStatus) {
        IO {
          scribe.info(
            s"Queued tasks: ${queueStat.queued.size}. Running tasks: ${queueStat.running.size}. Pending nodes: ${state.nodes.pending.size} . Running nodes: ${state.nodes.running.size}. Largest request: ${queueStat.queued
                .sortBy(_._2.cpu)
                .lastOption}/${queueStat.queued.sortBy(_._2.memory).lastOption}"
          )
        }
      } else IO.unit
      val (newState, io) =
        try {
          val neededNodes = decideNewNode.needNewNode(
            queueStat,
            state.nodes.running.toSeq.map(_._2) ++ Seq(unmanagedResource),
            state.nodes.pending.toSeq.map(_._2)
          )

          val skip = neededNodes.values.sum == 0
          if (!skip) {
            val canRequest =
              config.maxNodes > (state.nodes.running.size + state.nodes.pending.size) &&
                state.nodes.cumulativeRequested <= config.maxNodesCumulative
            if (!canRequest) {
              state -> IO(
                scribe.info(
                  "New node request will not proceed: pending nodes or reached max nodes. max: " + config.maxNodes + ", pending: " + state.nodes.pending.size + ", running: " + state.nodes.running.size
                )
              )
            } else {

              val allowedNewNodes = math.min(
                config.maxNodes - (state.nodes.running.size + state.nodes.pending.size),
                config.maxNodesCumulative - state.nodes.cumulativeRequested
              )

              val requestedNodes = neededNodes.take(allowedNewNodes)

              val updatedState: IO[Unit] = IO(
                scribe.info(
                  "Request " + requestedNodes.size + " node. One from each: " + requestedNodes.keySet
                )
              ) *> IO
                .parSequenceN(1)(requestedNodes.toList.map {
                  case (request, _) =>
                    createNode
                      .requestOneNewJobFromJobScheduler(request)
                      .flatMap {
                        case Left(e) =>
                          IO(
                            scribe.warn(
                              "Request failed: " + e + " " + e
                            )
                          ) *>
                            ref.update(
                              _.update(
                                NodeEvent(NodeRegistryState.NodeRequested)
                              )
                            )
                        case Right((jobId, size)) =>
                          IO(
                            scribe.info(
                              s"Request succeeded. Job id: $jobId, size: $size"
                            )
                          ) *> ref.update(
                            _.update(NodeEvent(NodeRegistryState.NodeRequested))
                              .update(
                                NodeEvent(
                                  NodeRegistryState.NodeIsPending(jobId, size)
                                )
                              )
                          ) *> IO
                            .sleep(config.pendingNodeTimeout)
                            .flatMap { initFailed =>
                              ref.flatModify { state =>
                                if (state.nodes.pending.contains(jobId)) {
                                  scribe.warn("Node init failed: " + jobId)

                                  state.update(
                                    NodeEvent(
                                      NodeRegistryState.InitFailed(jobId)
                                    )
                                  ) ->
                                    shutdownNode.shutdownPendingNode(jobId)

                                } else (state, IO.unit)
                              }
                            }
                            .start
                            .void

                      }
                })
                .void

              (state, updatedState)

            }
          } else (state, IO.unit)

        } catch {
          case e: Exception =>
            (state, IO(scribe.error(e, "Error during requesting node")))
        }

      newState -> logIO *> io
    }

  private def handleLauncherStopped(
      launcher: LauncherName
  ) = IO(scribe.info(s"LauncherStopped: $launcher")) *> ref.flatModify {
    state =>
      import tasks.util.eq._
      val msgs =
        state.scheduledTasks.toSeq.filter(_._2._1 === launcher).map(_._1)
      val updated = msgs.foldLeft(state) { (state, schProjection) =>
        val (_, _, proxies, sch) = state.scheduledTasks(schProjection)
        state.update(TaskLauncherStoppedFor(sch)).update(Enqueued(sch, proxies))
      }

      val negotiatingWithStoppedLauncher = state.negotiation.exists {
        case (negotiatingLauncher, _) =>
          (negotiatingLauncher: LauncherName) == (launcher: LauncherName)
      }
      if (negotiatingWithStoppedLauncher) {
        scribe.error(
          "Launcher stopped during negotiation phase. Automatic recovery from this is not implemented. The scheduler is deadlocked and it should be restarted."
        )
      }
      val updated2 = updated.update(LauncherCrashed(launcher))
      val node = state.knownLaunchers.get(launcher).flatten
      val shutdown = node
        .flatMap { node =>
          shutdownNode.map { shutdownNode =>
            shutdownNode.shutdownRunningNode(node.name)
          }
        }
        .getOrElse(IO.unit)
      (updated2 -> shutdown)
  } *> handleQueueStatIO

  def askForWork(
      launcher: LauncherName,
      availableResource: VersionedResourceAvailable,
      node: Option[Node]
  ): IO[Either[MessageData.NothingForSchedule.type, MessageData.Schedule]] = {

    val askIO = ref.flatModify { state =>
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
                val st2 =
                  withNegotiation.update(LauncherJoined(launcher, node))
                st2 -> node
                  .flatMap(n =>
                    createNode.map(createNode => handleNewNode(n, createNode))
                  )
                  .getOrElse(IO.unit)
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

    handleQueueStatIO *> askIO

  }

  def ack(
      allocated: VersionedResourceAllocated,
      launcher: LauncherName
  ): IO[Unit] =
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
          .update(TaskScheduled(sch, launcher, allocated))
      } else state
    }

  def taskSuccess(
      sch: ScheduleTask,
      resultWithMetadata: UntypedResultWithMetadata,
      elapsedTime: ElapsedTimeNanoSeconds,
      resourceAllocated: ResourceAllocated
  ): IO[Unit] = {
    val taskSuccessIO = ref.flatModify { state =>
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
  taskSuccessIO *> handleQueueStatIO
}

  def taskFailed(sch: ScheduleTask, cause: Throwable): IO[Unit] =
    {
      val taskFailedIO = ref.flatModify { state =>
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
    taskFailedIO *> handleQueueStatIO
  }

}
