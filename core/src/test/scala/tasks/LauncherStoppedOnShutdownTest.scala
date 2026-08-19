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
import tasks.JvmElasticSupport.JvmGrid
import tasks.queue._
import tasks.shared._
import tasks.util.SessionId
import tasks.util.message._

object LauncherStoppedOnShutdownTest extends TestHelpers {

  val testTask = Task[Input, Int]("launcherStoppedOnShutdown", 1) { _ => _ =>
    IO(1)
  }

  val heartbeat = "tasks.failuredetector.heartbeat-interval = 120 s"

  val externalQueueConfig = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      tasks.cache.enabled = false
      hosts.numCPU = 0
      tasks.disableRemoting = false
      tasks.addShutdownHook = false
      tasks.elastic.maxNodes = 1
      tasks.elastic.idleNodeTimeout = 3 seconds
      tasks.askInterval = 250 millis
      tasks.elastic.pendingNodeTimeout = 300 s
      $heartbeat
      """
    )
  }

  val workerConfig =
    s"""
    tasks.elastic.idleNodeTimeout = 3 seconds
    tasks.askInterval = 250 millis
    $heartbeat
    """

  private def waitUntilNoNode(
      ref: Ref[IO, QueueImpl.State]
  ): IO[QueueImpl.State] = {
    def loop(remaining: Int): IO[QueueImpl.State] = ref.get.flatMap { state =>
      val workerLaunchers = state.knownLaunchers.count(_._2.isDefined)
      if (
        (state.nodes.running.isEmpty && workerLaunchers == 0) || remaining <= 0
      )
        IO.pure(state)
      else IO.sleep(250.millis) *> loop(remaining - 1)
    }
    loop(80)
  }

  def run = Ref
    .of[IO, QueueImpl.State](QueueImpl.State.empty)
    .flatMap { queueStateRef =>
      JvmGrid.make(Some(queueStateRef), workerConfig).use {
        case (_, elasticSupport) =>
          withTaskSystem(
            config = Some(externalQueueConfig),
            s3Client = Resource.pure(None),
            elasticSupport = Resource.pure(Some(elasticSupport)),
            externalQueueState = Resource.pure(
              Some(tasks.util.Transaction.fromRef(queueStateRef))
            )
          ) { implicit ts =>
            for {
              result <- testTask(Input(1))(tasks.ResourceRequest(1, 500))
              whileUp <- queueStateRef.get
              afterIdleShutdown <- waitUntilNoNode(queueStateRef)
            } yield (whileUp, afterIdleShutdown, result)
          }
      }
    }

}

class LauncherStoppedOnShutdownTestSuite extends FunSuite with Matchers {

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Warn))
    .replace()

  private val session = "session0000000000"

  private def proxy(suffix: String) =
    Proxy(Address(SessionId.tag(session, s"ProxyTask-t.1-1-$suffix"), None))

  private def scheduleTask(name: String, hash: String, p: Proxy) =
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
      proxy = p.address,
      filePrefix = "file-prefix"
    )

  private val untypedResult =
    UntypedResult(Set.empty, Base64Data("cmVzdWx0"), None)

  private def stateWithQueued(sch: MessageData.ScheduleTask, ps: List[Proxy]) =
    QueueImpl.State(
      queuedTasks = Map(QueueImpl.project(sch) -> ((sch, ps))),
      scheduledTasks = Map.empty,
      knownLaunchers = Map.empty,
      counters = Map.empty,
      nodes = NodeRegistryState.State.empty,
      rendezvous = Map.empty,
      completedResults = Map.empty
    )

  test(
    "a completion that lands after the task was re-enqueued clears the queued entry"
  ) {
    val p = proxy("a")
    val sch = scheduleTask("raced", "raced-hash", p)
    val state = stateWithQueued(sch, List(p))

    state.queuedTasks.keySet should contain(QueueImpl.project(sch))

    val after = state.update(
      QueueImpl.TaskDone(
        sch,
        UntypedResultWithMetadata(untypedResult, null, false),
        ElapsedTimeNanoSeconds(1L),
        ResourceAllocated(1, 500, 0, Nil, None)
      )
    )

    after.queuedTasks shouldBe empty
    after.scheduledTasks shouldBe empty
  }

  test("a failure that lands after the task was re-enqueued clears it too") {
    val p = proxy("b")
    val sch = scheduleTask("racedfail", "racedfail-hash", p)
    val state = stateWithQueued(sch, List(p))

    state.update(QueueImpl.TaskFailed(sch)).queuedTasks shouldBe empty
  }

  test("proxiesOf finds the waiting proxies in whichever map holds the task") {
    val p = proxy("c")
    val sch = scheduleTask("lookup", "lookup-hash", p)

    stateWithQueued(sch, List(p)).proxiesOf(sch) shouldBe List(p)

    val scheduled = QueueImpl.State(
      queuedTasks = Map.empty,
      scheduledTasks = Map(
        QueueImpl.project(sch) -> (
          (
            LauncherName("l"),
            VersionedResourceAllocated(
              CodeVersion("v1"),
              ResourceAllocated(1, 500, 0, Nil, None)
            ),
            List(p),
            sch
          )
        )
      ),
      knownLaunchers = Map.empty,
      counters = Map.empty,
      nodes = NodeRegistryState.State.empty,
      rendezvous = Map.empty,
      completedResults = Map.empty
    )
    scheduled.proxiesOf(sch) shouldBe List(p)

    QueueImpl.State.empty.proxiesOf(sch) shouldBe Nil
  }

  test(
    "a worker that shuts down on idle removes its node and its launcher without waiting for the failure detector"
  ) {
    val (whileUp, afterIdleShutdown, result) =
      LauncherStoppedOnShutdownTest.run
        .unsafeRunTimed(120.seconds)
        .getOrElse(throw new RuntimeException("timeout"))
        .toOption
        .get

    result shouldBe 1

    whileUp.nodes.running.size shouldBe 1
    whileUp.knownLaunchers.count(_._2.isDefined) shouldBe 1

    afterIdleShutdown.nodes.running shouldBe empty
    afterIdleShutdown.knownLaunchers.count(_._2.isDefined) shouldBe 0
    afterIdleShutdown.scheduledTasks shouldBe empty
    afterIdleShutdown.queuedTasks shouldBe empty
  }

}
