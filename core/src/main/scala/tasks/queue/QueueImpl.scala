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
import tasks.shared.ResourceAvailable
import tasks.shared.ResourceRequest
import tasks.util.config.TasksConfig
import tasks.util.message.LauncherName
import tasks.shared.VersionedResourceAvailable
import tasks.util.HeartBeatIO
import tasks.util.eq._
import tasks.shared.VersionedResourceAllocated
import cats.effect.kernel.Ref
import cats.effect.FiberIO
import cats.effect.std.Mutex
import tasks.util.Transaction
import cats.effect.kernel.Resource
import tasks.elastic.NodeRegistryState

import tasks.elastic.ShutdownNode
import tasks.elastic.DecideNewNode
import tasks.elastic.CreateNode
import tasks.shared.RunningJobId
import tasks.util.CorrelationId
import scribe.LogFeature

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
  case class LauncherJoined(launcher: LauncherName, node: Option[Node])
      extends Event
  case class LauncherAvailabilityUpdated(
      launcher: LauncherName,
      available: VersionedResourceAvailable
  ) extends Event
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
  case class CancelRequested(sch: ScheduleTask) extends Event
  case class Preempted(sch: ScheduleTask) extends Event

  /** Outcome of a preemption decision tick. */
  sealed trait PreemptionDecision
  object PreemptionDecision {

    /** Either there's no queued work or no scheduled work, or there is
      * a scheduled task without descendants (so the system isn't fully
      * blocked on its own children), or some launcher already has free
      * capacity for at least one queued task. No action needed.
      */
    case object NotStalled extends PreemptionDecision

    /** The system is stalled but no candidate ancestor on any single
      * launcher could free enough resources for any queued task.
      * Increments `stalledUnresolvable`.
      */
    case object Unresolvable extends PreemptionDecision

    /** A viable preemption was found. Caller should emit
      * `CancelRequested` events and send `CancelTask` messages.
      */
    case class Cancel(
        launcher: LauncherName,
        victims: List[(ScheduleTaskEqualityProjection, ScheduleTask)]
    ) extends PreemptionDecision
  }

  /** Stall: |Q| > 0 ∧ ∀ t ∈ S. ∃ u ∈ Q∪S with invocationId(t) ∈ anc(u).
    * Pick deepest valid ancestor first; fall back to multi-victim on
    * one launcher.
    */
  def selectPreemptionVictims(state: State): PreemptionDecision = {
    val stalled =
      state.queuedTasks.nonEmpty && state.scheduledTasks.nonEmpty
    val launcherAlreadyHasRoom =
      state.queuedTasks.valuesIterator.exists { case (q, _) =>
        state.availableResourcesByLauncher.valuesIterator.exists(
          _.canFulfillRequest(q.resource)
        )
      }
    if (!stalled || launcherAlreadyHasRoom) PreemptionDecision.NotStalled
    else {
      val invocationIdOfScheduled: Map[
        TaskInvocationId,
        ScheduleTaskEqualityProjection
      ] = state.scheduledTasks.iterator.flatMap { case (k, (_, _, _, sch)) =>
        val invId =
          TaskInvocationId(sch.description.taskId, sch.description)
        Iterator.single(invId -> k)
      }.toMap

      val invocationIdsAppearingInLineage: Set[TaskInvocationId] =
        (state.queuedTasks.valuesIterator.map(_._1) ++
          state.scheduledTasks.valuesIterator.map(_._4))
          .flatMap(_.lineage.lineage.iterator)
          .toSet

      val everyScheduledHasDescendant = state.scheduledTasks.valuesIterator
        .forall { case (_, _, _, sch) =>
          val invId =
            TaskInvocationId(sch.description.taskId, sch.description)
          invocationIdsAppearingInLineage.contains(invId)
        }

      if (!everyScheduledHasDescendant) PreemptionDecision.NotStalled
      else {
        val queuedSortedDeepestFirst = state.queuedTasks.valuesIterator
          .map(_._1)
          .toList
          .sortBy(-_.lineage.lineage.length)

        def candidateChain(
            q: ScheduleTask
        ): List[
          (
              ScheduleTaskEqualityProjection,
              ScheduleTask,
              LauncherName,
              VersionedResourceAllocated
          )
        ] =
          q.lineage.lineage.reverse.iterator
            .flatMap { invId =>
              invocationIdOfScheduled.get(invId).iterator.flatMap { k =>
                state.scheduledTasks.get(k).iterator.map {
                  case (l, alloc, _, sch) => (k, sch, l, alloc)
                }
              }
            }
            .filterNot { case (k, _, _, _) =>
              state.cancelInFlight.contains(k)
            }
            .toList

        def trySingleVictim(
            q: ScheduleTask
        ): Option[
          (LauncherName, List[(ScheduleTaskEqualityProjection, ScheduleTask)])
        ] =
          candidateChain(q).iterator
            .flatMap { case (k, sch, l, alloc) =>
              state.availableResourcesByLauncher.get(l).iterator.flatMap {
                available =>
                  if (available.addBack(alloc).canFulfillRequest(q.resource))
                    Iterator.single((l, List(k -> sch)))
                  else Iterator.empty
              }
            }
            .nextOption()

        def tryMultiVictim(
            q: ScheduleTask
        ): Option[
          (LauncherName, List[(ScheduleTaskEqualityProjection, ScheduleTask)])
        ] =
          candidateChain(q)
            .groupBy(_._3)
            .iterator
            .flatMap { case (l, entries) =>
              state.availableResourcesByLauncher.get(l).iterator.flatMap {
                initialAvailable =>
                  val (finalAvailable, pickedReverse) =
                    entries.foldLeft(
                      (
                        initialAvailable,
                        List.empty[
                          (ScheduleTaskEqualityProjection, ScheduleTask)
                        ]
                      )
                    ) { case ((a, acc), (k, sch, _, alloc)) =>
                      if (a.canFulfillRequest(q.resource)) (a, acc)
                      else (a.addBack(alloc), (k -> sch) :: acc)
                    }
                  val picked = pickedReverse.reverse
                  if (
                    picked.size > 1 &&
                    finalAvailable.canFulfillRequest(q.resource)
                  )
                    Iterator.single((l, picked))
                  else Iterator.empty
              }
            }
            .nextOption()

        queuedSortedDeepestFirst.iterator
          .flatMap(q =>
            trySingleVictim(q).orElse(tryMultiVictim(q)).iterator
          )
          .nextOption() match {
          case None =>
            PreemptionDecision.Unresolvable
          case Some((launcher, victims)) =>
            PreemptionDecision.Cancel(launcher, victims)
        }
      }
    }
  }

  def project(sch: ScheduleTask) =
    ScheduleTaskEqualityProjection(sch.description)

  /** Queue-side state.
    *
    * @param availableResourcesByLauncher
    *   Per-launcher snapshot of free CPU/memory/etc. Overwritten on
    *   every askForWork from each launcher, and subtracted from on
    *   TaskScheduled so the view stays close between asks. Not added
    *   back on TaskDone/Failed/etc. Self-corrects on the
    *   next askForWork. Used only by preemption decisions, never as
    *   the source of truth for scheduling.
    * @param cancelInFlight
    *   Tasks for which the queue has sent CancelTask to a launcher but
    *   hasn't yet received TaskPreemptedAck. Prevents repeated cancels
    *   of the same victim within one preemption decision tick.
    */
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
      counters: Map[LauncherName, Long],
      nodes: NodeRegistryState.State,
      availableResourcesByLauncher: Map[LauncherName, VersionedResourceAvailable] =
        Map.empty,
      cancelInFlight: Set[ScheduleTaskEqualityProjection] = Set.empty
  ) {

    private def availableMinus(
        launcher: LauncherName,
        alloc: VersionedResourceAllocated
    ): Map[LauncherName, VersionedResourceAvailable] =
      availableResourcesByLauncher.get(launcher) match {
        case Some(a) =>
          availableResourcesByLauncher.updated(launcher, a.substract(alloc))
        case None => availableResourcesByLauncher
      }

    def update(e: Event): State = {
      e match {
        case NodeEvent(ev) =>
          copy(nodes = nodes.update(ev))
        case Incremented(launcher) =>
          copy(counters = counters.get(launcher) match {
            case None        => counters.updated(launcher, 1L)
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
        case LauncherJoined(launcher, node) =>
          copy(knownLaunchers = knownLaunchers + (launcher -> node))
        case LauncherAvailabilityUpdated(launcher, available) =>
          copy(
            availableResourcesByLauncher =
              availableResourcesByLauncher.updated(launcher, available)
          )
        case TaskScheduled(sch, launcher, allocated) =>
          val (_, proxies) = queuedTasks(project(sch))
          copy(
            queuedTasks = queuedTasks - project(sch),
            scheduledTasks = scheduledTasks
              .updated(project(sch), (launcher, allocated, proxies, sch)),
            availableResourcesByLauncher = availableMinus(launcher, allocated)
          )

        case TaskDone(sch, _, _, _) =>
          copy(
            scheduledTasks = scheduledTasks - project(sch),
            cancelInFlight = cancelInFlight - project(sch)
          )
        case TaskFailed(sch) =>
          copy(
            scheduledTasks = scheduledTasks - project(sch),
            cancelInFlight = cancelInFlight - project(sch)
          )
        case TaskLauncherStoppedFor(sch) =>
          copy(
            scheduledTasks = scheduledTasks - project(sch),
            cancelInFlight = cancelInFlight - project(sch)
          )
        case LauncherCrashed(launcher) =>
          copy(
            knownLaunchers = knownLaunchers - launcher,
            counters = counters - launcher,
            availableResourcesByLauncher =
              availableResourcesByLauncher - launcher
          )
        case CacheHit(sch, _) =>
          copy(
            scheduledTasks = scheduledTasks - project(sch),
            cancelInFlight = cancelInFlight - project(sch)
          )
        case CancelRequested(sch) =>
          copy(cancelInFlight = cancelInFlight + project(sch))
        case Preempted(sch) =>
          copy(
            scheduledTasks = scheduledTasks - project(sch),
            cancelInFlight = cancelInFlight - project(sch)
          )

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

  }

  object State {
    def empty =
      State(Map(), Map(), Map(), Map(), NodeRegistryState.State.empty)

  }

  private[tasks] def fromTransaction(
      transaction: Transaction[QueueImpl.State],
      cache: TaskResultCache,
      messenger: Messenger,
      shutdownNode: Option[tasks.elastic.ShutdownNode],
      decideNewNode: Option[tasks.elastic.DecideNewNode],
      createNode: Option[tasks.elastic.CreateNode],
      unmanagedResource: tasks.shared.ResourceAvailable,
      meterProvider: org.typelevel.otel4s.metrics.MeterProvider[IO]
  )(implicit config: TasksConfig): Resource[IO, QueueImpl] = {
    QueueMetrics.make(meterProvider, transaction.get).flatMap { metrics =>
      Resource.make(
        Mutex[IO].flatMap(handleQueueStatMutex =>
          Ref.of[IO, List[FiberIO[Unit]]](Nil).flatMap { ref =>
            val q = new QueueImpl(
              ref = transaction,
              fiberList = ref,
              cache = cache,
              messenger = messenger,
              shutdownNode = shutdownNode,
              decideNewNode = decideNewNode,
              createNode = createNode,
              unmanagedResource = unmanagedResource,
              metrics = metrics,
              handleQueueStatMutex = handleQueueStatMutex
            )
            q.startCounterLoops.map(_ => q)
          }
        )
      )(_.release)
    }
  }

  private[tasks] def initRef(
      cache: TaskResultCache,
      messenger: Messenger,
      shutdownNode: Option[tasks.elastic.ShutdownNode],
      decideNewNode: Option[tasks.elastic.DecideNewNode],
      createNode: Option[tasks.elastic.CreateNode],
      unmanagedResource: tasks.shared.ResourceAvailable,
      meterProvider: org.typelevel.otel4s.metrics.MeterProvider[IO]
  )(implicit
      config: TasksConfig
  ) =
    Resource.eval(Ref.of[IO, QueueImpl.State](QueueImpl.State.empty)).flatMap {
      stateRef =>
        QueueMetrics.make(meterProvider, stateRef.get).flatMap { metrics =>
          Resource.make(
            Mutex[IO].flatMap(handleQueueStatMutex =>
              Ref.of[IO, List[FiberIO[Unit]]](Nil).flatMap { ref2 =>
                val q = new QueueImpl(
                  ref = Transaction.fromRef(stateRef),
                  fiberList = ref2,
                  cache = cache,
                  messenger = messenger,
                  shutdownNode = shutdownNode,
                  decideNewNode = decideNewNode,
                  createNode = createNode,
                  unmanagedResource = unmanagedResource,
                  metrics = metrics,
                  handleQueueStatMutex = handleQueueStatMutex
                )
                q.startCounterLoops.map(_ => q)
              }
            )
          )(_.release)
        }
    }
}

private[tasks] class QueueImpl(
    ref: Transaction[QueueImpl.State],
    fiberList: Ref[IO, List[FiberIO[Unit]]],
    cache: TaskResultCache,
    messenger: Messenger,
    shutdownNode: Option[tasks.elastic.ShutdownNode],
    decideNewNode: Option[tasks.elastic.DecideNewNode],
    createNode: Option[tasks.elastic.CreateNode],
    unmanagedResource: tasks.shared.ResourceAvailable,
    metrics: QueueMetrics,
    handleQueueStatMutex: Mutex[IO]
)(implicit config: TasksConfig) {
  import QueueImpl._

  def initFailed(n: RunningJobId): IO[Unit] = {
    val handleFailureIO = createNode match {
      case None => IO.unit
      case Some(value) =>
        value.convertRunningToPending(n).flatMap {
          case Some(pending) =>
            ref.update(
              _.update(NodeEvent(NodeRegistryState.InitFailed(pending)))
            )
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
          IO(
            scribe.debug(
              s"Query counter",
              launcher,
              scribe.data(
                "explain",
                "if the request times out then we assume the launcher is stopped"
              )
            )
          ) *>
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
          s"Releasing resources held by TasksQueue.",
          scribe.data("fiber-count", fibers.size)
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
                scribe.info("Shutting down running node ", node)
                shutdownNode.shutdownRunningNode(node)

              } ++
                st.nodes.pending.map { case (node, _) =>
                  scribe.info("Shutting down pending node ", node)
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
      a: tasks.caching.AnswerFromCache,
      allProxies: List[Proxy]
  ): IO[Unit] = {
    val message = a.message
    val sch = a.sch
    val num = CorrelationId.make
    scribe.debug(s"Cache answered.", sch, num, a.sender.address)
    val cacheIO = message match {
      case Right(Some(result)) => {
        scribe.debug(
          s"Result found.",
          sch,
          num,
          scribe.data("explain", "replying with result found in cache")
        )
        ref.flatModify { state =>
          val sends = allProxies.map(p =>
            messenger.submit(
              Message(
                MessageData.MessageFromTask(result, retrievedFromCache = true),
                from = Address.unknown,
                to = p.address
              )
            )
          )
          state.update(CacheHit(sch, result)) ->
            (metrics.onCacheHit(sch.description) *> IO
              .parSequenceN(1)(sends)
              .void)

        }
      }
      case Right(None) => {
        scribe.debug(
          s"NotFound",
          sch,
          num,
          scribe.data("explain", "Task is not found in cache. Enqueue.")
        )
        ref.update(_.update(Enqueued(sch, allProxies))) *> metrics
          .onEnqueued(sch.description)
      }
      case Left(msg) => {
        scribe.debug(
          s"NotFound",
          sch,
          num,
          scribe.data("explain", "Task is not found in cache. Enqueue."),
          scribe.data("message", msg)
        )

        ref.update(_.update(Enqueued(sch, allProxies))) *> metrics
          .onEnqueued(sch.description)
      }

    }
    cacheIO *> handleQueueStatIO
  }

  
  private def enqueueOrCacheHit(
      sch: ScheduleTask,
      proxies: List[Proxy],
      state: State
  ): (State, IO[Unit]) = {
    if (sch.tryCache && proxies.nonEmpty) {
      val sender = proxies.head
      val effect = cache
        .checkResult(sch, sender)
        .flatMap(r => handleCacheAnwser(r, proxies))
        .start
        .void
      (state, effect)
    } else {
      scribe.debug(
        "AvoidCache",
        sch,
        scribe.data(
          "explain",
          "ScheduleTask should not be checked in the cache. Enqueue."
        )
      )
      (
        state.update(Enqueued(sch, proxies)),
        metrics.onEnqueued(sch.description)
      )
    }
  }

  def scheduleTask(sch: ScheduleTask): IO[Unit] = {
    val scheduleIO = ref.flatModify { state =>
      scribe.debug(s"ScheduleTask", sch)
      val proxy = Proxy(sch.proxy)

      if (state.queuedButSentByADifferentProxy(sch, proxy)) {
        state.update(Enqueued(sch, List(proxy))) -> metrics.onEnqueued(
          sch.description
        )
      } else if (state.scheduledButSentByADifferentProxy(sch, proxy)) {
        scribe.debug(
          s"MultipleProxies",
          sch,
          scribe.data(
            "explain",
            "Scheduletask received multiple times from different proxies. Not queueing this one, but delivering result if ready. "
          )
        )
        state.update(ProxyAddedToScheduledMessage(sch, List(proxy))) -> IO.unit

      } else {
        enqueueOrCacheHit(sch, List(proxy), state)
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
        scribe.warn(s"Failed to convert back to pending", runningId)
        IO.unit
    }
  }

  private lazy val handleQueueStatIO =
    handleQueueStatMutex.lock.surround(
      createNode
        .flatMap { createNode =>
          shutdownNode
            .flatMap { shn =>
              decideNewNode.map { dn => handleQueueStat(shn, dn, createNode) }
            }
        }
        .getOrElse(IO.unit) *> tryPreemptIO
    )

  /** Stall detection + victim selection. Runs under handleQueueStatMutex so
    * reads of Q, S, availableResourcesByLauncher, cancelInFlight are
    * consistent. 
    */
  private lazy val tryPreemptIO: IO[Unit] =
    ref.flatModify { state =>
      selectPreemptionVictims(state) match {
        case PreemptionDecision.NotStalled =>
          (state, IO.unit)
        case PreemptionDecision.Unresolvable =>
          (state, metrics.onPreemptionStallUnresolvable)
        case PreemptionDecision.Cancel(launcher, victims) =>
          val newState = victims.foldLeft(state) { case (s, (_, sch)) =>
            s.update(CancelRequested(sch))
          }
          val sendCancels = IO
            .parSequenceN(1)(victims.map { case (_, sch) =>
              messenger.submit(
                Message(
                  MessageData.CancelTask(sch),
                  from = Address.unknown,
                  to = Address(launcher.name)
                )
              )
            })
            .void
          val incrementCancellations =
            victims.foldLeft(IO.unit) { case (acc, (_, sch)) =>
              acc *> metrics.onPreemptionCancellation(sch.description)
            }
          val logIO = IO(
            scribe.info(
              "PreemptStall",
              launcher,
              scribe.data(
                Map(
                  "victims" -> victims.size,
                  "queued-tasks" -> state.queuedTasks.size,
                  "scheduled-tasks" -> state.scheduledTasks.size,
                  "explain" ->
                    "Detected stall (all scheduled tasks blocked on queued descendants). Cancelling parents to free resources."
                )
              )
            )
          )
          val cacheWarn =
            if (!config.cacheEnabled)
              IO(
                scribe.warn(
                  "PreemptStallWithCacheDisabled",
                  launcher,
                  scribe.data(
                    "explain",
                    "Preemption fired while tasks.cache.enabled=false. " +
                      "Cancelled parents will re-execute their children on " +
                      "rerun (no cache hit available); enable cache for " +
                      "release-resources-early-equivalent semantics."
                  )
                )
              )
            else IO.unit
          (
            newState,
            cacheWarn *> logIO *> sendCancels *> incrementCancellations *>
              metrics.onPreemptionStallResolved
          )
      }
    }

  /** Handle the launcher's ack of a cancel request. Removes the task from
    * scheduledTasks and re-enqueues (or delivers a cache hit if the cache
    * was populated by a concurrent natural completion).
    */
  def taskPreempted(sch: ScheduleTask): IO[Unit] = {
    val preemptedIO = ref.flatModify { state =>
      state.scheduledTasks.get(project(sch)) match {
        case None =>
          scribe.debug(
            "PreemptAckNoEntry",
            sch,
            scribe.data(
              "explain",
              "Preempted ack arrived but the scheduled entry is already gone. " +
                "Most likely a natural completion won the race. " +
                "Cleared cancelInFlight only."
            )
          )
          state.update(Preempted(sch)) -> IO.unit
        case Some((_, _, proxies, oldSch)) =>
          val afterPreempt = state.update(Preempted(oldSch))
          val (finalState, effect) =
            enqueueOrCacheHit(oldSch, proxies, afterPreempt)
          val logIO = IO(
            scribe.info(
              "Preempted",
              sch,
              scribe.data(
                Map(
                  "proxies" -> proxies.size,
                  "explain" ->
                    "Launcher acked cancel. Re-routing task through the cache/queue path."
                )
              )
            )
          )
          finalState -> (logIO *> effect)
      }
    }
    preemptedIO *> handleQueueStatIO
  }

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
          scribe.debug(
            s"Queue state report.",
            scribe.data(
              Map(
                "queued-tasks" -> queueStat.queued.size,
                "running-tasks" -> queueStat.running.size,
                "pending-nodes" -> state.nodes.pending.size,
                "running-nodes" -> state.nodes.running.size
              )
            )
          )
        }
      } else IO.unit
      val (newState, io) =
        try {
          val rawNeededNodes = decideNewNode.needNewNode(
            queueStat,
            state.nodes.running.toSeq.map(_._2) ++ Seq(unmanagedResource),
            state.nodes.pending.toSeq.map(_._2)
          )

          def committedResourceFor(req: ResourceRequest): ResourceAvailable =
            ResourceAvailable(
              cpu = req.cpu._1,
              memory = req.memory,
              scratch = req.scratch,
              gpu = (0 until req.gpu).toList,
              image = req.image
            )

          val inFlightByShape: Map[ResourceAvailable, Int] =
            state.nodes.inFlightRequests
              .groupBy(identity)
              .map { case (k, v) => k -> v.size }

          val neededNodes: Map[ResourceRequest, Int] =
            rawNeededNodes.flatMap { case (req, count) =>
              val alreadyInFlight =
                inFlightByShape.getOrElse(committedResourceFor(req), 0)
              val adjusted = count - alreadyInFlight
              if (adjusted > 0) Some(req -> adjusted) else None
            }

          val skip = neededNodes.values.sum == 0
          if (!skip) {
            val activeOrInFlight =
              state.nodes.running.size + state.nodes.pending.size + state.nodes.inFlightRequests.size
            val canRequest =
              config.maxNodes > activeOrInFlight &&
                state.nodes.cumulativeRequested < config.maxNodesCumulative
            if (!canRequest) {
              state -> IO(
                if (config.logQueueStatus) {
                  scribe.debug(
                    "MaxNodesReached",
                    scribe.data(
                      Map(
                        "max-nodes" -> config.maxNodes,
                        "max-nodes-cumulative" -> config.maxNodesCumulative,
                        "cumulative-requested" -> state.nodes.cumulativeRequested,
                        "in-flight-requests" -> state.nodes.inFlightRequests.size,
                        "pending-nodes" -> state.nodes.pending.size,
                        "running-nodes" -> state.nodes.running.size,
                        "explain" -> "New node request will not proceed because pending nodes or reached max nodes."
                      )
                    )
                  )
                }
              )
            } else {

              val allowedNewNodes = math.min(
                config.maxNodes - activeOrInFlight,
                config.maxNodesCumulative - state.nodes.cumulativeRequested
              )

              val requestedList: List[ResourceRequest] =
                neededNodes.toList
                  .flatMap { case (req, count) => List.fill(count)(req) }
                  .take(allowedNewNodes)

              val preCommittedState =
                requestedList.foldLeft(state) { (s, req) =>
                  s.update(
                    NodeEvent(
                      NodeRegistryState.NodeRequested(committedResourceFor(req))
                    )
                  )
                }

              val updatedState: IO[Unit] = IO(
                scribe.info(
                  "RequestNodes",
                  scribe.data(
                    Map(
                      "request-node-count" -> requestedList.size,
                      "request-node-resources" -> requestedList
                        .groupBy(identity)
                        .view
                        .mapValues(_.size)
                        .toMap
                        .toString,
                      "queued-tasks" -> state.queuedTasks.size,
                      "running-tasks" -> state.scheduledTasks.size,
                      "running-nodes" -> state.nodes.running.size,
                      "pending-nodes" -> state.nodes.pending.size,
                      "in-flight-requests" -> state.nodes.inFlightRequests.size,
                      "cumulative-requested" -> state.nodes.cumulativeRequested,
                      "max-nodes" -> config.maxNodes,
                      "max-nodes-cumulative" -> config.maxNodesCumulative
                    )
                  )
                )
              ) *> IO
                .parSequenceN(1)(requestedList.map { request =>
                  val committedResource = committedResourceFor(request)
                  val recordFailure: IO[Unit] = ref.update(
                    _.update(
                      NodeEvent(
                        NodeRegistryState.NodeRequestFailed(committedResource)
                      )
                    )
                  )
                  IO.uncancelable { poll =>
                    poll(
                      createNode.requestOneNewJobFromJobScheduler(request)
                    ).flatMap {
                      case Left(e) =>
                        IO(
                          scribe.warn(
                            "NodeRequestFailed",
                            scribe.data("info", e),
                            scribe.data(
                              "explain",
                              "This is normal if there is no more capacity. " +
                                "Note: failed requests still count against maxNodesCumulative " +
                                "as a defensive measure to bound total attempts."
                            )
                          )
                        ) *> recordFailure
                      case Right((jobId, size)) =>
                        ref.flatModify { state =>
                          val updated = state.update(
                            NodeEvent(
                              NodeRegistryState.NodeIsPending(
                                jobId,
                                size,
                                committedResource
                              )
                            )
                          )
                          val logIO = IO(
                            scribe.info(
                              "NodeRequestSucceeded",
                              jobId,
                              size,
                              scribe.data(
                                Map(
                                  "queued-tasks" -> updated.queuedTasks.size,
                                  "running-tasks" -> updated.scheduledTasks.size,
                                  "running-nodes" -> updated.nodes.running.size,
                                  "pending-nodes" -> updated.nodes.pending.size,
                                  "in-flight-requests" -> updated.nodes.inFlightRequests.size,
                                  "cumulative-requested" -> updated.nodes.cumulativeRequested
                                )
                              )
                            )
                          )
                          (updated, logIO)
                        } *> IO
                          .sleep(config.pendingNodeTimeout)
                          .flatMap { initFailed =>
                            ref.flatModify { state =>
                              if (state.nodes.pending.contains(jobId)) {
                                scribe.warn(
                                  "NodeInitFailed: ",
                                  jobId,
                                  scribe.data(
                                    "explain",
                                    "The node was allocated but the peer process on the node failed to make initial contact."
                                  )
                                )

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
                  }.guaranteeCase {
                    case cats.effect.Outcome.Canceled() =>
                      IO(
                        scribe.warn(
                          "NodeRequestCancelled",
                          scribe.data(
                            "explain",
                            "Pre-committed in-flight slot is being released " +
                              "because the requesting IO was cancelled before " +
                              "the scheduler could respond."
                          )
                        )
                      ) *> recordFailure
                    case _ => IO.unit
                  }
                })
                .void

              (preCommittedState, updatedState)

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
  ) = ref.flatModify { state =>
    import tasks.util.eq._
    val msgs =
      state.scheduledTasks.toSeq.filter(_._2._1 === launcher).map(_._1)
    val (updated, reEnqueued) =
      msgs.foldLeft((state, List.empty[ScheduleTask])) {
        case ((state, acc), schProjection) =>
          val (_, _, proxies, sch) = state.scheduledTasks(schProjection)
          (
            state
              .update(TaskLauncherStoppedFor(sch))
              .update(Enqueued(sch, proxies)),
            sch :: acc
          )
      }

    val node = state.knownLaunchers.get(launcher).flatten
    val updated2 = {
      val st1 = updated.update(LauncherCrashed(launcher))
      node.fold(st1)(n =>
        st1.update(NodeEvent(NodeRegistryState.NodeIsDown(n)))
      )
    }

    val shutdown = node
      .flatMap { node =>
        shutdownNode.map { shutdownNode =>
          shutdownNode.shutdownRunningNode(node.name)
        }
      }
      .getOrElse(IO.unit)
    val recordMetrics =
      reEnqueued.foldLeft(IO.unit)((acc, sch) =>
        acc *> metrics.onEnqueued(sch.description)
      )
    val logIO = IO(
      scribe.info(
        "LauncherStopped",
        launcher,
        scribe.data(
          Map(
            "re-enqueued-tasks" -> reEnqueued.size,
            "re-enqueued-tasks-by-id" -> msgs
              .groupBy(_.description.taskId)
              .view
              .mapValues(_.size)
              .toMap
              .toString,
            "had-node" -> node.isDefined,
            "queued-tasks-after" -> updated2.queuedTasks.size,
            "scheduled-tasks-after" -> updated2.scheduledTasks.size,
            "running-nodes-after" -> updated2.nodes.running.size,
            "pending-nodes" -> updated2.nodes.pending.size,
            "in-flight-requests" -> updated2.nodes.inFlightRequests.size
          )
        )
      )
    )
    (updated2 -> (logIO *> recordMetrics *> shutdown))
  } *> handleQueueStatIO

  def askForWork(
      launcher: LauncherName,
      availableResource: VersionedResourceAvailable,
      node: Option[Node]
  ): IO[Either[MessageData.NothingForSchedule.type, MessageData.Schedule]] = {

    val askIO = ref.flatModify { state =>
      val num = CorrelationId.make
      scribe.debug(
        s"AskForWork",
        scribe.data(
          "queued-tasks",
          state.queuedTasks.map { case (_, (sch, _)) =>
            (sch.description.taskId, sch.resource)
          }.toSeq
        ),
        availableResource,
        num,
        launcher,
        node.map(v => Node.toLogFeature(v)).getOrElse(scribe.data(Map.empty))
      )

      val invocationIdsAppearingInLineage: Set[TaskInvocationId] =
        (state.queuedTasks.valuesIterator.map(_._1) ++
          state.scheduledTasks.valuesIterator.map(_._4))
          .flatMap(_.lineage.lineage.iterator)
          .toSet

      var maxPrio = Int.MinValue
      var maxDepth = Int.MinValue
      var selected = Option.empty[ScheduleTask]
      state.queuedTasks.valuesIterator
        .foreach { case (sch, _) =>
          val invId =
            TaskInvocationId(sch.description.taskId, sch.description)
          val hasPendingDescendant =
            invocationIdsAppearingInLineage.contains(invId)
          val ret = availableResource.canFulfillRequest(sch.resource)
          if (ret && !hasPendingDescendant) {
            val prio = sch.priority.s
            val depth = sch.lineage.lineage.length
            val better =
              prio > maxPrio || (prio == maxPrio && depth > maxDepth)
            if (better) {
              maxPrio = prio
              maxDepth = depth
              selected = Some(sch)
            }
          } else if (!ret) {
            scribe.debug(
              s"CantFulfillRequest",
              num,
              sch,
              availableResource,
              scribe.data(
                "explain",
                "No available resources for this task"
              )
            )
          }
        }

      // Register the launcher / mark the node up on FIRST contact, regardless of
      // whether there is work to hand it. Doing this inside the `Some(sch)`
      // branch (the previous behaviour) meant a worker that came up while the
      // queue was momentarily empty was never recorded as up, so its pending
      // entry would hit `pendingNodeTimeout` and trigger `NodeInitFailed` even
      // though the worker was fully alive and polling for work.
      val isNewLauncher = !state.knownLaunchers.contains(launcher)
      val stateWithJoin =
        if (isNewLauncher) state.update(LauncherJoined(launcher, node))
        else state
      // Refresh the launcher's free-resource view on every ask so that
      // tryPreempt has an accurate picture to decide cancellations on.
      val stateWithRefreshedAvailable =
        stateWithJoin.update(
          LauncherAvailabilityUpdated(launcher, availableResource)
        )
      val joinIO =
        if (isNewLauncher)
          node
            .flatMap(n =>
              createNode.map(createNode => handleNewNode(n, createNode))
            )
            .getOrElse(IO.unit)
        else IO.unit

      selected match {
        case None =>
          scribe.debug(
            s"FoundNothingToSchedule",
            num,
            launcher,
            availableResource,
            scribe.data("queued-tasks", state.queuedTasks)
          )
          stateWithRefreshedAvailable -> (joinIO *> IO.pure(
            Left(MessageData.NothingForSchedule)
          ))

        case Some(sch) =>
          val allocated = availableResource.maximum(sch.resource)
          scribe.debug(
            s"Dequeue",
            num,
            sch,
            launcher,
            allocated,
            scribe.data("explain", "Scheduling task to launcher.")
          )

          val newState = stateWithRefreshedAvailable
            .update(TaskScheduled(sch, launcher, allocated))

          val io =
            metrics.onTaskScheduled(sch.description) *> joinIO *> IO.pure(
              Right(MessageData.Schedule(sch))
            )

          newState -> io

      }
    }

    askIO <* handleQueueStatIO

  }

  def taskSuccess(
      sch: ScheduleTask,
      resultWithMetadata: UntypedResultWithMetadata,
      elapsedTime: ElapsedTimeNanoSeconds,
      resourceAllocated: ResourceAllocated
  ): IO[Unit] = {
    val taskSuccessIO = ref.flatModify { state =>
      scribe.debug(s"TaskDone", sch, resultWithMetadata)
      val recordMetric = metrics.onTaskDone(sch.description, elapsedTime.s)
      if (state.queuedTasks.contains(project(sch))) {
        scribe.error(
          s"ShouldNotBeQueued.",
          scribe.data(
            "explain",
            "This completed task is in the list of queued tasks. It was removed from the list of queued tasks when it was scheduled. Thus it was submitted twice to the queue."
          ),
          state.queuedTasks(project(sch))._1,
          scribe.data(
            Map(
              "proxies" -> state.queuedTasks(project(sch))._2.map(_.address)
            )
          )
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
      ) -> (recordMetric *> io)
    }
    taskSuccessIO *> handleQueueStatIO
  }

  def taskFailed(sch: ScheduleTask, cause: Throwable): IO[Unit] = {
    val taskFailedIO = ref.flatModify { state =>
      val recordMetric = metrics.onTaskFailed(sch.description)
      val (updated, sideEffects) = state.scheduledTasks
        .get(project(sch))
        .foldLeft((state, List.empty[IO[Unit]])) {
          case ((state, sideEffectAcc), (_, _, proxies, _)) =>
            val removed = state.update(TaskFailed(sch))
            if (config.resubmitFailedTask) {
              scribe.error(
                cause,
                "TaskExecutionFailed+Resubmit",
                sch,
                scribe.data(
                  "explain",
                  "configuration tasks.resubmitFailedTask=false can prevent this"
                ),
                scribe.data("queue-size", state.queuedTasks.keys.size)
              )

              (
                removed.update(Enqueued(sch, proxies)),
                sideEffectAcc :+ metrics.onEnqueued(sch.description)
              )
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
              scribe.error(
                cause,
                "TaskExecutionFailed",
                sch,
                scribe.data(
                  "explain",
                  "configuration tasks.resubmitFailedTask=true can resubmit automatically"
                )
              )
              (removed, sideEffectAcc ++ sideEffects)
            }
        }
      updated -> (recordMetric *> IO.parSequenceN(1)(sideEffects).void)
    }
    taskFailedIO *> handleQueueStatIO
  }

}
