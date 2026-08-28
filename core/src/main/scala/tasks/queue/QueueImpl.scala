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
import tasks.util.message.RendezvousGroupId
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
import tasks.elastic.ConvertRunningToPending
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
  case class SessionProxiesDropped(session: String) extends Event
  case class MainProcessJoined(session: String) extends Event
  case class MainProcessLeft(session: String) extends Event

  sealed trait LauncherStopReason {
    def proxiesCanNoLongerPoll: Boolean
    def shouldRequestReplacementNodes: Boolean
  }
  object LauncherStopReason {
    case object SelfReportedByThisProcess extends LauncherStopReason {
      def proxiesCanNoLongerPoll = true
      def shouldRequestReplacementNodes = false
    }
    case object SelfReportedByRemoteProcess extends LauncherStopReason {
      def proxiesCanNoLongerPoll = true
      def shouldRequestReplacementNodes = true
    }
    case object TimedOutByFailureDetector extends LauncherStopReason {
      def proxiesCanNoLongerPoll = false
      def shouldRequestReplacementNodes = true
    }
  }
  case class CacheHit(sch: ScheduleTask, result: UntypedResult) extends Event
  case class NodeEvent(ev: NodeRegistryState.Event) extends Event
  case class RendezvousJoined(
      groupId: RendezvousGroupId,
      rank: Int,
      worldSize: Int,
      payload: String
  ) extends Event

  case class ResultStoredForProxy(proxy: Address, result: ProxyResult)
      extends Event
  case class ResultDeliveredToProxy(proxy: Address) extends Event
  case class RendezvousRead(groupId: RendezvousGroupId, rank: Int) extends Event

  case class RendezvousGroup(
      worldSize: Int,
      joiners: Map[Int, String],
      readers: Set[Int]
  )

  sealed trait ProxyResult
  case class ProxyResultSuccess(
      result: UntypedResult,
      retrievedFromCache: Boolean
  ) extends ProxyResult
  case class ProxyResultFailure(cause: Throwable) extends ProxyResult

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
      counters: Map[LauncherName, Long],
      nodes: NodeRegistryState.State,
      rendezvous: Map[RendezvousGroupId, RendezvousGroup] = Map.empty,
      completedResults: Map[Address, ProxyResult],
      mainProcesses: Set[String]
  ) {

    def freeCapacityOfRunningNodes: List[ResourceAvailable] = {
      val nodeOfLauncher: Map[LauncherName, RunningJobId] =
        knownLaunchers.collect { case (launcher, Some(node)) =>
          launcher -> node.name
        }
      val allocationsByNode: Map[RunningJobId, List[ResourceAllocated]] =
        scheduledTasks.values.toList
          .flatMap { case (launcher, allocated, _, _) =>
            nodeOfLauncher
              .get(launcher)
              .map(_ -> allocated.cpuMemoryAllocated)
          }
          .groupBy(_._1)
          .map { case (runningJobId, pairs) =>
            runningJobId -> pairs.map(_._2)
          }

      nodes.running.toList.map { case (runningJobId, total) =>
        allocationsByNode
          .getOrElse(runningJobId, Nil)
          .foldLeft(total)((free, allocated) => free.substract(allocated))
      }
    }

    def update(e: Event): State = {
      e match {
        case NodeEvent(ev) =>
          copy(nodes = nodes.update(ev))
        case RendezvousJoined(groupId, rank, worldSize, payload) =>
          val group = rendezvous
            .getOrElse(
              groupId,
              RendezvousGroup(worldSize, Map.empty, Set.empty)
            )
          copy(rendezvous =
            rendezvous.updated(
              groupId,
              group.copy(joiners = group.joiners.updated(rank, payload))
            )
          )

        case RendezvousRead(groupId, rank) =>
          rendezvous.get(groupId) match {
            case None => this
            case Some(group) =>
              val withReader = group.copy(readers = group.readers + rank)
              if (withReader.readers.size >= group.worldSize)
                copy(rendezvous = rendezvous - groupId)
              else copy(rendezvous = rendezvous.updated(groupId, withReader))
          }
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
        case TaskScheduled(sch, launcher, allocated) =>
          val (_, proxies) = queuedTasks(project(sch))
          copy(
            queuedTasks = queuedTasks - project(sch),
            scheduledTasks = scheduledTasks
              .updated(project(sch), (launcher, allocated, proxies, sch))
          )

        case TaskDone(sch, _, _, _) =>
          copy(
            scheduledTasks = scheduledTasks - project(sch),
            queuedTasks = queuedTasks - project(sch)
          )
        case TaskFailed(sch) =>
          copy(
            scheduledTasks = scheduledTasks - project(sch),
            queuedTasks = queuedTasks - project(sch)
          )
        case TaskLauncherStoppedFor(sch) =>
          copy(scheduledTasks = scheduledTasks - project(sch))
        case LauncherCrashed(launcher) =>
          copy(
            knownLaunchers = knownLaunchers - launcher,
            counters = counters - launcher
          )

        case SessionProxiesDropped(session) =>
          def alive(proxy: Proxy) =
            !tasks.util.SessionId.belongsTo(proxy.address.value, session)
          copy(
            queuedTasks = queuedTasks.map { case (key, (sch, proxies)) =>
              (key, (sch, proxies.filter(alive)))
            },
            scheduledTasks = scheduledTasks.map {
              case (key, (launcher, allocated, proxies, sch)) =>
                (key, (launcher, allocated, proxies.filter(alive), sch))
            },
            completedResults = completedResults.filterNot { case (address, _) =>
              tasks.util.SessionId.belongsTo(address.value, session)
            }
          )
        case CacheHit(sch, _) =>
          copy(scheduledTasks = scheduledTasks - project(sch))

        case ResultStoredForProxy(proxy, result) =>
          copy(completedResults = completedResults.updated(proxy, result))

        case ResultDeliveredToProxy(proxy) =>
          copy(completedResults = completedResults - proxy)

        case MainProcessJoined(session) =>
          copy(mainProcesses = mainProcesses + session)

        case MainProcessLeft(session) =>
          copy(mainProcesses = mainProcesses - session)

      }
    }

    def proxiesOf(sch: ScheduleTask): List[Proxy] = {
      val scheduled = scheduledTasks
        .get(project(sch))
        .toList
        .flatMap { case (_, _, proxies, _) => proxies }
      val queued = queuedTasks
        .get(project(sch))
        .toList
        .flatMap { case (_, proxies) => proxies }
      (scheduled ::: queued).distinct
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
      State(
        queuedTasks = Map(),
        scheduledTasks = Map(),
        knownLaunchers = Map(),
        counters = Map(),
        nodes = NodeRegistryState.State.empty,
        rendezvous = Map(),
        completedResults = Map(),
        mainProcesses = Set()
      )

  }

  private[tasks] def fromTransaction(
      transaction: Transaction[QueueImpl.State],
      cache: TaskResultCache,
      messenger: Messenger,
      shutdownNode: Option[tasks.elastic.ShutdownNode],
      decideNewNode: Option[tasks.elastic.DecideNewNode],
      createNode: Option[tasks.elastic.CreateNode],
      convertRunningToPending: Option[tasks.elastic.ConvertRunningToPending],
      unmanagedResource: tasks.shared.ResourceAvailable,
      meterProvider: org.typelevel.otel4s.metrics.MeterProvider[IO],
      mainProcessSession: Option[String],
      onFatalError: IO[Unit] = IO.unit
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
              convertRunningToPending = convertRunningToPending,
              unmanagedResource = unmanagedResource,
              metrics = metrics,
              handleQueueStatMutex = handleQueueStatMutex,
              mainProcessSession = mainProcessSession,
              onFatalError = onFatalError
            )
            q.joinAsMainProcess *> q.startCounterLoops.map(_ => q)
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
      convertRunningToPending: Option[tasks.elastic.ConvertRunningToPending],
      unmanagedResource: tasks.shared.ResourceAvailable,
      meterProvider: org.typelevel.otel4s.metrics.MeterProvider[IO],
      mainProcessSession: Option[String],
      onFatalError: IO[Unit] = IO.unit
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
                  convertRunningToPending = convertRunningToPending,
                  unmanagedResource = unmanagedResource,
                  metrics = metrics,
                  handleQueueStatMutex = handleQueueStatMutex,
                  mainProcessSession = mainProcessSession,
                  onFatalError = onFatalError
                )
                q.joinAsMainProcess *> q.startCounterLoops.map(_ => q)
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
    convertRunningToPending: Option[tasks.elastic.ConvertRunningToPending],
    unmanagedResource: tasks.shared.ResourceAvailable,
    metrics: QueueMetrics,
    handleQueueStatMutex: Mutex[IO],
    mainProcessSession: Option[String],
    onFatalError: IO[Unit] = IO.unit
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
    def round: IO[Unit] = ref.get
      .map(_.knownLaunchers.keySet.toList)
      .flatMap { launchers =>
        if (launchers.isEmpty)
          IO.sleep(config.launcherActorHeartBeatInterval)
        else
          IO.parSequenceN(1)(
            launchers.map { launcher =>
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
                  sideEffect = handleLauncherStopped(
                    launcher,
                    LauncherStopReason.TimedOutByFailureDetector
                  )
                )
            }
          ).void
      }

    def loop: IO[Unit] = round.attempt
      .flatMap {
        case Left(e) =>
          IO(
            scribe.error(
              "Error reading counters and/or handling stopped launchers",
              e
            )
          ) *> IO.sleep(config.launcherActorHeartBeatInterval)
        case Right(_) => IO.unit
      }
      .flatMap(_ => loop)

    loop.start.flatMap { fiber => fiberList.update(list => fiber :: list) }
  }

  private[tasks] def joinAsMainProcess: IO[Unit] = mainProcessSession match {
    case None => IO.unit
    case Some(session) =>
      ref.flatModify { state =>
        val joined = state.update(MainProcessJoined(session))
        joined -> IO(
          scribe.info(
            "MainProcessJoined",
            scribe.data(
              Map(
                "session" -> session,
                "main-processes" -> joined.mainProcesses.toList.sorted.toString
              )
            )
          )
        )
      }
  }

  private def leaveAsMainProcess: IO[Unit] = mainProcessSession match {
    case None => IO.unit
    case Some(session) =>
      ref.flatModify { state =>
        val left = state.update(MainProcessLeft(session))
        if (
          left.mainProcesses.isEmpty && config.clearQueueStateWhenLastMainProcessExits
        )
          State.empty -> IO(
            scribe.info(
              "QueueStateCleared",
              scribe.data(
                Map(
                  "session" -> session,
                  "explain" -> "This was the last main process using this queue state, so it was emptied. The next run starts from a clean slate.",
                  "discarded-queued-tasks" -> left.queuedTasks.size,
                  "discarded-scheduled-tasks" -> left.scheduledTasks.size,
                  "discarded-known-launchers" -> left.knownLaunchers.size,
                  "discarded-completed-results" -> left.completedResults.size,
                  "discarded-cumulative-requested" -> left.nodes.cumulativeRequested
                )
              )
            )
          )
        else
          left -> IO(
            scribe.info(
              "MainProcessLeft",
              scribe.data(
                Map(
                  "session" -> session,
                  "explain" -> "Other main processes are still using this queue state, so it was left intact.",
                  "main-processes" -> left.mainProcesses.toList.sorted.toString
                )
              )
            )
          )
      }
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

    IO.both(stopFibers, stopNodes).void *> leaveAsMainProcess
  }

  private def handleCacheAnwser(
      a: tasks.caching.AnswerFromCache,
      allProxies: List[Proxy]
  ): IO[Unit] = {
    val message = a.message
    val sch = a.sch
    val num = CorrelationId.make
    scribe.debug(s"Cache answered.", sch, num, a.sender.address)
    val enqueueIO = ref.flatModify { state =>
      state.update(Enqueued(sch, allProxies)) ->
        warnIfResourceRequestDiverges(sch, state)
    } *> metrics.onEnqueued(sch.description)
    val cacheIO = message match {
      case Right(Some(result)) => {
        scribe.debug(
          s"Result found.",
          sch,
          num,
          scribe.data("explain", "replying with result found in cache")
        )
        ref.flatModify { state =>
          val stored =
            allProxies.foldLeft(state.update(CacheHit(sch, result))) {
              case (acc, p) =>
                acc.update(
                  ResultStoredForProxy(
                    p.address,
                    ProxyResultSuccess(result, retrievedFromCache = true)
                  )
                )
            }
          stored -> metrics.onCacheHit(sch.description)

        }
      }
      case Right(None) => {
        scribe.debug(
          s"NotFound",
          sch,
          num,
          scribe.data("explain", "Task is not found in cache. Enqueue.")
        )
        enqueueIO
      }
      case Left(msg) => {
        scribe.debug(
          s"NotFound",
          sch,
          num,
          scribe.data("explain", "Task is not found in cache. Enqueue."),
          scribe.data("message", msg)
        )

        enqueueIO
      }

    }
    cacheIO *> handleQueueStatIO
  }

  private def warnIfResourceRequestDiverges(
      sch: ScheduleTask,
      stateBeforeEnqueue: State
  ): IO[Unit] =
    stateBeforeEnqueue.queuedTasks.get(project(sch)) match {
      case Some((alreadyQueued, _)) if alreadyQueued.resource != sch.resource =>
        IO(
          scribe.warn(
            "ResourceRequestDiverges",
            sch.description,
            scribe.data(
              Map(
                "kept-resource-request" -> sch.resource.toString,
                "discarded-resource-request" -> alreadyQueued.resource.toString,
                "explain" ->
                  ("The same task was submitted again with a different resource request. Task identity is the task id and the hash of its input, so the resource request is not part of it and the newest submission replaces the stored one. Every proxy waiting on this task now depends on the kept request, including submitters which asked for something smaller. If no node can satisfy the kept request the task never gets scheduled and all of them wait forever. Put whatever varies the resource request into the task input if the variants are meant to be different tasks.")
              )
            )
          )
        )
      case _ => IO.unit
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
        warnIfResourceRequestDiverges(sch, state) *> metrics.onEnqueued(
          sch.description
        )
      )
    }
  }

  def scheduleTask(sch: ScheduleTask): IO[Unit] = {
    val scheduleIO = ref.flatModify { state =>
      scribe.debug(s"ScheduleTask", sch)
      val proxy = Proxy(sch.proxy)

      if (state.queuedButSentByADifferentProxy(sch, proxy)) {
        state.update(Enqueued(sch, List(proxy))) ->
          (warnIfResourceRequestDiverges(sch, state) *> metrics.onEnqueued(
            sch.description
          ))
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

  def rendezvous(
      groupId: RendezvousGroupId,
      rank: Int,
      worldSize: Int,
      payload: String
  ): IO[List[String]] = {
    def loop: IO[List[String]] =
      rendezvousStep(groupId, rank, worldSize, payload).flatMap {
        case Some(peers) => IO.pure(peers)
        case None        => IO.sleep(config.rendezvousPollInterval) *> loop
      }
    loop
  }

  def rendezvousStep(
      groupId: RendezvousGroupId,
      rank: Int,
      worldSize: Int,
      payload: String
  ): IO[Option[List[String]]] = ref.flatModify { state =>
    def fatal(reason: String): (State, IO[Option[List[String]]]) = {
      scribe.error(
        s"RendezvousInvariantViolation",
        scribe.data(
          Map(
            "group-id" -> groupId.value,
            "offending-rank" -> rank,
            "offending-world-size" -> worldSize,
            "offending-payload" -> payload,
            "reason" -> reason
          )
        )
      )
      (
        state,
        onFatalError *> IO.raiseError(new RuntimeException(reason))
      )
    }

    def readyOrNot(s: State): (State, IO[Option[List[String]]]) = {
      val g = s.rendezvous(groupId)
      if (g.joiners.size == worldSize) {
        val peers = (0 until worldSize).toList.map(g.joiners(_))
        (s.update(RendezvousRead(groupId, rank)), IO.pure(Some(peers)))
      } else (s, IO.pure(None))
    }

    if (worldSize <= 0)
      fatal(s"worldSize must be positive, got $worldSize")
    else if (rank < 0 || rank >= worldSize)
      fatal(s"rank $rank out of range for worldSize $worldSize")
    else
      state.rendezvous.get(groupId) match {
        case Some(existing) if existing.worldSize != worldSize =>
          fatal(
            s"worldSize mismatch on group ${groupId.value}: existing=${existing.worldSize} new=$worldSize"
          )
        case Some(existing)
            if existing.joiners.get(rank).exists(_ != payload) =>
          fatal(s"duplicate rank $rank in group ${groupId.value}")
        case Some(existing) if existing.joiners.contains(rank) =>
          readyOrNot(state)
        case _ =>
          readyOrNot(
            state.update(RendezvousJoined(groupId, rank, worldSize, payload))
          )
      }
  }

  private def handleNewNode(
      node: Node,
      convert: ConvertRunningToPending
  ): IO[Unit] = {
    val runningId = node.name
    convert.convertRunningToPending(runningId).flatMap {
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
        .getOrElse(IO.unit)
    )

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
          val plannedSpawns = decideNewNode.needNewNode(
            queueStat,
            state.freeCapacityOfRunningNodes ++ Seq(unmanagedResource),
            state.nodes.pending.toSeq.map(_._2)
          )
          val noWorkerKnown =
            state.knownLaunchers.isEmpty &&
              state.nodes.running.isEmpty &&
              state.nodes.pending.isEmpty &&
              state.nodes.inFlightRequests.isEmpty
          val rawNeededNodes: Map[ResourceRequest, Int] =
            if (plannedSpawns.nonEmpty) plannedSpawns
            else if (queueStat.queued.nonEmpty && noWorkerKnown)
              queueStat.queued.headOption.map { case (_, versioned) =>
                versioned.cpuMemoryRequest -> 1
              }.toMap
            else plannedSpawns

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

  private[tasks] def handleLauncherStopped(
      launcher: LauncherName,
      reason: LauncherStopReason
  ): IO[Unit] = ref.flatModify { state =>
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
    val session = tasks.util.SessionId.of(launcher.name)
    val updated2 = {
      val st1 = updated.update(LauncherCrashed(launcher))
      val st2 =
        if (reason.proxiesCanNoLongerPoll)
          session.fold(st1)(s => st1.update(SessionProxiesDropped(s)))
        else st1
      node.fold(st2)(n =>
        st2.update(NodeEvent(NodeRegistryState.NodeIsDown(n)))
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
            "session" -> session.getOrElse("none"),
            "dropped-completed-results" -> (updated.completedResults.size - updated2.completedResults.size),
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
  } *> (if (reason.shouldRequestReplacementNodes) handleQueueStatIO
        else IO.unit)

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

      val eligible = state.queuedTasks.valuesIterator
        .map(_._1)
        .filter { sch =>
          val invId =
            TaskInvocationId(sch.description.taskId, sch.description)
          val hasPendingDescendant =
            invocationIdsAppearingInLineage.contains(invId)
          val ret = availableResource.canFulfillRequest(sch.resource)
          if (!ret) {
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
          ret && !hasPendingDescendant
        }
        .toList

      val selected = eligible.maxByOption { sch =>
        val request = sch.resource.cpuMemoryRequest
        (
          sch.priority.s,
          request.gpu,
          request.cpu._1,
          request.memory,
          request.scratch,
          sch.lineage.lineage.length
        )
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
      val joinIO =
        if (isNewLauncher)
          node
            .flatMap(n =>
              convertRunningToPending.map(convert => handleNewNode(n, convert))
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
          stateWithJoin -> (joinIO *> IO.pure(
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

          val newState = stateWithJoin
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
        scribe.warn(
          s"CompletedWhileQueued",
          scribe.data(
            "explain",
            "This completed task was back in the queue, most likely because its launcher was reported stopped while the task was finishing. The result is delivered to the waiting proxies and the queued entry is dropped, so the task is not executed a second time."
          ),
          state.queuedTasks(project(sch))._1,
          scribe.data(
            Map(
              "proxies" -> state.queuedTasks(project(sch))._2.map(_.address)
            )
          )
        )
      }
      val proxies = state.proxiesOf(sch)

      val done = state.update(
        TaskDone(sch, resultWithMetadata, elapsedTime, resourceAllocated)
      )

      val stored = proxies.foldLeft(done) { case (acc, pr) =>
        acc.update(
          ResultStoredForProxy(
            pr.address,
            ProxyResultSuccess(
              resultWithMetadata.untypedResult,
              retrievedFromCache = false
            )
          )
        )
      }

      stored -> recordMetric
    }
    taskSuccessIO *> handleQueueStatIO
  }

  def pollResult(proxy: Address): IO[Option[ProxyResult]] =
    ref.flatModify { state =>
      state.completedResults.get(proxy) match {
        case None => state -> IO.pure(Option.empty[ProxyResult])
        case Some(result) =>
          scribe.debug(
            s"ResultPolled",
            scribe.data("proxy", proxy.toString)
          )
          state.update(ResultDeliveredToProxy(proxy)) -> IO.pure(Some(result))
      }
    }

  def taskFailed(sch: ScheduleTask, cause: Throwable): IO[Unit] = {
    val taskFailedIO = ref.flatModify { state =>
      val recordMetric = metrics.onTaskFailed(sch.description)
      val proxies = state.proxiesOf(sch)
      val known = state.scheduledTasks.contains(project(sch)) ||
        state.queuedTasks.contains(project(sch))
      val (updated, sideEffects) =
        if (!known) (state, List.empty[IO[Unit]])
        else {
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
              List(metrics.onEnqueued(sch.description))
            )
          } else {
            val stored = proxies.foldLeft(removed) { case (acc, pr) =>
              acc.update(
                ResultStoredForProxy(pr.address, ProxyResultFailure(cause))
              )
            }
            scribe.error(
              cause,
              "TaskExecutionFailed",
              sch,
              scribe.data(
                "explain",
                "configuration tasks.resubmitFailedTask=true can resubmit automatically"
              )
            )
            (stored, List.empty[IO[Unit]])
          }
        }
      updated -> (recordMetric *> IO.parSequenceN(1)(sideEffects).void)
    }
    taskFailedIO *> handleQueueStatIO
  }

}
