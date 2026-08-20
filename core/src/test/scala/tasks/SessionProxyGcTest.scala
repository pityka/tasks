package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.kernel.Deferred
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import org.ekrich.config.ConfigFactory
import scala.concurrent.duration._

import tasks.elastic.NodeRegistryState
import tasks.fileservice.FileServicePrefix
import tasks.jsonitersupport._
import tasks.queue._
import tasks.shared._
import tasks.util.SessionId
import tasks.util.message._

object SessionProxyGcTest extends TestHelpers {

  val taskStarted = Deferred[IO, Unit].unsafeRunSync()
  val releaseTask = Deferred[IO, Unit].unsafeRunSync()

  val blockingTask = Task[Input, Int]("sessionProxyGc", 1) { _ => _ =>
    taskStarted.complete(()) *> releaseTask.get.map(_ => 1)
  }

  val externalQueueConfig = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      tasks.cache.enabled = false
      hosts.numCPU = 4
      tasks.disableRemoting = false
      tasks.addShutdownHook = false
      """
    )
  }

  def run = Ref
    .of[IO, QueueImpl.State](QueueImpl.State.empty)
    .flatMap { queueStateRef =>
      withTaskSystem(
        config = Some(externalQueueConfig),
        s3Client = Resource.pure(None),
        elasticSupport = Resource.pure(None),
        externalQueueState = Resource.pure(
          Some(tasks.util.Transaction.fromRef(queueStateRef))
        )
      ) { implicit ts =>
        for {
          fiber <- blockingTask(Input(1))(tasks.ResourceRequest(1, 500)).start
          _ <- taskStarted.get
          state <- queueStateRef.get
          _ <- releaseTask.complete(())
          result <- fiber.joinWithNever
        } yield (state, result)
      }
    }

}

class SessionProxyGcTestSuite extends FunSuite with Matchers {

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Warn))
    .replace()

  implicit val tasksConfig: tasks.util.config.TasksConfig =
    tasks.util.config.parse(() => ConfigFactory.load())

  private val deadSession = "deadSession000000"
  private val liveSession = "liveSession111111"

  private def proxyOf(session: String, suffix: String) =
    Proxy(Address(SessionId.tag(session, s"ProxyTask-name.1-1-$suffix"), None))

  private val untaggedProxy =
    Proxy(Address("ProxyTask-name.1-1-legacy", None))

  private def scheduleTask(name: String, hash: String, proxy: Proxy) =
    MessageData.ScheduleTask(
      description = HashedTaskDescription(TaskId(name, 1), hash),
      inputDeserializer = Spore[AnyRef, AnyRef]("some.pkg.Deserializer$", Nil),
      outputSerializer = Spore[AnyRef, AnyRef]("some.pkg.Serializer$", Nil),
      function = Spore[AnyRef, AnyRef]("some.pkg.Jobs$body$1", Nil),
      resource = VersionedResourceRequest(
        CodeVersion("v1"),
        ResourceRequest((1, 1), 500, 0, 0, None)
      ),
      input = MessageData.InputData(Base64Data("aGVsbG8="), false),
      fileServicePrefix = FileServicePrefix(Vector("prefix")),
      tryCache = true,
      priority = Priority(0),
      labels = Labels(Nil),
      lineage = TaskLineage(Nil),
      proxy = proxy.address,
      filePrefix = "file-prefix"
    )

  private val result =
    QueueImpl.ProxyResultSuccess(
      UntypedResult(Set.empty, Base64Data("cmVzdWx0"), None),
      retrievedFromCache = false
    )

  test(
    "SessionId round trips a tag and reports none for an untagged value"
  ) {
    SessionId.of(SessionId.tag("abc123", "Launcher-x")) shouldBe Some("abc123")
    SessionId.of(
      "Launcher-SimpleSocketAddress(127.0.1.1,40693)"
    ) shouldBe None
    SessionId.of(SessionId.tag("abc123", "a~b~c")) shouldBe Some("abc123")
    SessionId.of("~leading") shouldBe None
    SessionId.belongsTo(SessionId.tag("abc", "x"), "abc") shouldBe true
    SessionId.belongsTo(SessionId.tag("abd", "x"), "abc") shouldBe false
    SessionId.belongsTo("untagged", "abc") shouldBe false
  }

  test(
    "SessionProxiesDropped removes the dead session's proxies and completed results and keeps everything else"
  ) {
    val deadProxy = proxyOf(deadSession, "dead")
    val liveProxy = proxyOf(liveSession, "live")

    val queued = scheduleTask("queued", "queued-hash", liveProxy)
    val scheduled = scheduleTask("scheduled", "scheduled-hash", liveProxy)

    val state = QueueImpl.State(
      queuedTasks = Map(
        QueueImpl.project(queued) -> (
          (
            queued,
            List(deadProxy, liveProxy, untaggedProxy)
          )
        )
      ),
      scheduledTasks = Map(
        QueueImpl.project(scheduled) -> (
          (
            LauncherName(SessionId.tag(liveSession, "Launcher-1")),
            VersionedResourceAllocated(
              CodeVersion("v1"),
              ResourceAllocated(1, 500, 0, Nil, None)
            ),
            List(deadProxy, liveProxy),
            scheduled
          )
        )
      ),
      knownLaunchers = Map.empty,
      counters = Map.empty,
      nodes = NodeRegistryState.State.empty,
      rendezvous = Map.empty,
      completedResults = Map(
        deadProxy.address -> result,
        liveProxy.address -> result,
        untaggedProxy.address -> result
      ),
      mainProcesses = Set.empty
    )

    val after = state.update(QueueImpl.SessionProxiesDropped(deadSession))

    after
      .queuedTasks(QueueImpl.project(queued))
      ._2 shouldBe List(liveProxy, untaggedProxy)

    after
      .scheduledTasks(QueueImpl.project(scheduled))
      ._3 shouldBe List(liveProxy)

    after.completedResults.keySet shouldBe Set(
      liveProxy.address,
      untaggedProxy.address
    )

    after.queuedTasks.keySet shouldBe state.queuedTasks.keySet
    after.scheduledTasks.keySet shouldBe state.scheduledTasks.keySet
  }

  test(
    "a launcher timed out by the failure detector keeps the proxies of its session"
  ) {
    val liveProxy = proxyOf(liveSession, "live")
    val queued = scheduleTask("queued", "queued-hash", liveProxy)
    val launcher = LauncherName(SessionId.tag(liveSession, "Launcher-1"))

    val state = QueueImpl.State(
      queuedTasks =
        Map(QueueImpl.project(queued) -> ((queued, List(liveProxy)))),
      scheduledTasks = Map.empty,
      knownLaunchers = Map(launcher -> None),
      counters = Map(launcher -> 1L),
      nodes = NodeRegistryState.State.empty,
      rendezvous = Map.empty,
      completedResults = Map(liveProxy.address -> result),
      mainProcesses = Set.empty
    )

    def afterStop(reason: QueueImpl.LauncherStopReason) =
      Ref.of[IO, QueueImpl.State](state).flatMap { stateRef =>
        tasks.util.LocalMessenger.make
          .flatMap { messenger =>
            QueueImpl.fromTransaction(
              transaction = tasks.util.Transaction.fromRef(stateRef),
              cache = null,
              messenger = messenger,
              shutdownNode = None,
              decideNewNode = None,
              createNode = None,
              convertRunningToPending = None,
              unmanagedResource = ResourceAvailable.empty,
              meterProvider =
                org.typelevel.otel4s.metrics.MeterProvider.noop[IO],
              mainProcessSession = None
            )
          }
          .use(_.handleLauncherStopped(launcher, reason)) *> stateRef.get
      }

    val (timedOut, selfReported) = (for {
      a <- afterStop(QueueImpl.LauncherStopReason.TimedOutByFailureDetector)
      b <- afterStop(QueueImpl.LauncherStopReason.SelfReportedByThisProcess)
    } yield (a, b)).unsafeRunSync()

    timedOut.queuedTasks(QueueImpl.project(queued))._2 shouldBe List(liveProxy)
    timedOut.completedResults.keySet shouldBe Set(liveProxy.address)
    timedOut.knownLaunchers shouldBe empty

    selfReported.queuedTasks(QueueImpl.project(queued))._2 shouldBe empty
    selfReported.completedResults shouldBe empty
  }

  test(
    "a task completing after a spurious reap still delivers its result to the waiting proxy"
  ) {
    val liveProxy = proxyOf(liveSession, "live")
    val launcher = LauncherName(SessionId.tag(liveSession, "Launcher-1"))
    val sch = scheduleTask("reaped", "reaped-hash", liveProxy)
      .copy(tryCache = false)

    val offered = VersionedResourceAvailable(
      CodeVersion("v1"),
      ResourceAvailable(
        cpu = 4,
        memory = 1000,
        scratch = 0,
        gpu = Nil,
        image = None
      )
    )

    val completed = UntypedResultWithMetadata(
      UntypedResult(Set.empty, Base64Data("cmVzdWx0"), None),
      ResultMetadata(
        dependencies = Nil,
        started = java.time.Instant.now,
        ended = java.time.Instant.now,
        logs = Nil,
        lineage = TaskLineage(Nil)
      ),
      noCache = true
    )

    val program = Ref.of[IO, QueueImpl.State](QueueImpl.State.empty).flatMap {
      stateRef =>
        tasks.util.LocalMessenger.make
          .flatMap { messenger =>
            QueueImpl.fromTransaction(
              transaction = tasks.util.Transaction.fromRef(stateRef),
              cache = null,
              messenger = messenger,
              shutdownNode = None,
              decideNewNode = None,
              createNode = None,
              convertRunningToPending = None,
              unmanagedResource = ResourceAvailable.empty,
              meterProvider =
                org.typelevel.otel4s.metrics.MeterProvider.noop[IO],
              mainProcessSession = None
            )
          }
          .use { q =>
            for {
              _ <- q.scheduleTask(sch)
              dispatched <- q.askForWork(launcher, offered, None)
              _ <- q.handleLauncherStopped(
                launcher,
                QueueImpl.LauncherStopReason.TimedOutByFailureDetector
              )
              _ <- q.taskSuccess(
                sch,
                completed,
                ElapsedTimeNanoSeconds(1L),
                ResourceAllocated(1, 500, 0, Nil, None)
              )
              polled <- q.pollResult(liveProxy.address)
            } yield (dispatched, polled)
          }
    }

    val (dispatched, polled) = program.unsafeRunSync()

    dispatched shouldBe Right(MessageData.Schedule(sch))
    polled shouldBe defined
  }

  test("dropping a session that owns nothing leaves the state unchanged") {
    val liveProxy = proxyOf(liveSession, "live")
    val queued = scheduleTask("queued", "queued-hash", liveProxy)

    val state = QueueImpl.State(
      queuedTasks =
        Map(QueueImpl.project(queued) -> ((queued, List(liveProxy)))),
      scheduledTasks = Map.empty,
      knownLaunchers = Map.empty,
      counters = Map.empty,
      nodes = NodeRegistryState.State.empty,
      rendezvous = Map.empty,
      completedResults = Map(liveProxy.address -> result),
      mainProcesses = Set.empty
    )

    state.update(QueueImpl.SessionProxiesDropped(deadSession)) shouldBe state
  }

  test(
    "a running task system tags its launcher and its proxies with the same session"
  ) {
    val (state, taskResult) = SessionProxyGcTest.run
      .unsafeRunTimed(120.seconds)
      .getOrElse(throw new RuntimeException("timeout"))
      .toOption
      .get

    taskResult shouldBe 1

    val launcherSessions =
      state.knownLaunchers.keys.map(l => SessionId.of(l.name)).toSet
    launcherSessions.size shouldBe 1
    launcherSessions.head shouldBe defined

    val proxies = state.scheduledTasks.values.flatMap(_._3).toList
    proxies should not be empty
    proxies.map(p => SessionId.of(p.address.value)).toSet shouldBe launcherSessions
  }

}
