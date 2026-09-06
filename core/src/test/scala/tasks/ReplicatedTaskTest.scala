package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import tasks.fileservice.FileServicePrefix
import tasks.queue._
import tasks.shared._
import tasks.util.LocalMessenger
import tasks.util.message._

class ReplicatedTaskTest extends FunSuite with Matchers {

  implicit val config: tasks.util.config.TasksConfig =
    tasks.util.config.parse(() => org.ekrich.config.ConfigFactory.load())

  private val cv = CodeVersion("test")

  private val zone = "ecs.availability-zone"

  private val description = HashedTaskDescription(TaskId("train", 1), "hash")

  private val proxyAddress = Address("proxy-train", None)

  private def worker(cpu: Int) =
    VersionedResourceAvailable(cv, ResourceAvailable(cpu, 4000, 0, Nil, None))

  private def workerIn(cpu: Int, value: String) =
    VersionedResourceAvailable(
      cv,
      ResourceAvailable(cpu, 4000, 0, Nil, None, Set(s"$zone:$value"))
    )

  private def task(replication: Option[Replication], cpu: Int = 4) =
    MessageData.ScheduleTask(
      description = description,
      inputDeserializer = Spore[AnyRef, AnyRef]("some.pkg.Deserializer$", Nil),
      outputSerializer = Spore[AnyRef, AnyRef]("some.pkg.Serializer$", Nil),
      function = Spore[AnyRef, AnyRef]("some.pkg.Jobs$body$1", Nil),
      resource = VersionedResourceRequest(
        cv,
        ResourceRequest((cpu, cpu), 100, 0, 0, None, None, replication)
      ),
      input = MessageData.InputData(Base64Data("aGVsbG8="), false),
      fileServicePrefix = FileServicePrefix(Vector("prefix")),
      tryCache = false,
      priority = Priority(0),
      labels = Labels(Nil),
      lineage = TaskLineage(Nil),
      proxy = proxyAddress,
      filePrefix = "file-prefix"
    )

  private def result =
    UntypedResultWithMetadata(
      UntypedResult(Set.empty, Base64Data("aGVsbG8="), None),
      ResultMetadata(
        Nil,
        java.time.Instant.now,
        java.time.Instant.now,
        Nil,
        TaskLineage(Nil)
      ),
      noCache = true
    )

  private def withQueue[A](
      sch: MessageData.ScheduleTask
  )(body: QueueImpl => IO[A]): A =
    LocalMessenger.make
      .flatMap { messenger =>
        QueueImpl.initRef(
          cache = null,
          messenger = messenger,
          shutdownNode = None,
          decideNewNode = None,
          createNode = None,
          convertRunningToPending = None,
          unmanagedResource = ResourceAvailable.empty,
          meterProvider = org.typelevel.otel4s.metrics.MeterProvider.noop[IO],
          mainProcessSession = None
        )
      }
      .use(q => q.scheduleTask(sch) *> body(q))
      .unsafeRunSync()

  private def took(
      answer: Either[MessageData.NothingForSchedule.type, MessageData.Schedule]
  ): Boolean = answer.isRight

  test("one submission is handed out up to the maximum") {
    val sch = task(Some(Replication(3)))

    val taken = withQueue(sch) { q =>
      List("w1", "w2", "w3", "w4")
        .foldLeft(IO.pure(List.empty[Boolean])) { (acc, name) =>
          acc.flatMap(soFar =>
            q.askForWork(LauncherName(name), worker(4), None)
              .map(answer => soFar :+ took(answer))
          )
        }
    }

    taken shouldBe List(true, true, true, false)
  }

  test("a task without replication keeps its single dispatch") {
    val sch = task(None)

    val taken = withQueue(sch) { q =>
      for {
        a <- q.askForWork(LauncherName("w1"), worker(4), None)
        b <- q.askForWork(LauncherName("w2"), worker(4), None)
      } yield (took(a), took(b))
    }

    taken shouldBe ((true, false))
  }

  test("one worker with room may run several copies of the same task") {
    val sch = task(Some(Replication(3)))

    val taken = withQueue(sch) { q =>
      List(16, 12, 8, 4)
        .foldLeft(IO.pure(List.empty[Boolean])) { (acc, free) =>
          acc.flatMap(soFar =>
            q.askForWork(LauncherName("w1"), worker(free), None)
              .map(answer => soFar :+ took(answer))
          )
        }
    }

    taken shouldBe List(true, true, true, false)
  }

  test("a copy finishing releases one slot, not every slot on that worker") {
    val sch = task(Some(Replication(2)))

    val (offeredAgain, delivered) = withQueue(sch) { q =>
      for {
        _ <- q.askForWork(LauncherName("w1"), worker(16), None)
        _ <- q.askForWork(LauncherName("w1"), worker(12), None)
        _ <- q.taskSuccess(
          sch,
          LauncherName("w1"),
          result,
          ElapsedTimeNanoSeconds(1L),
          ResourceAllocated(4, 100, 0, Nil, None)
        )
        polled <- q.pollResult(proxyAddress)
        again <- q.askForWork(LauncherName("w2"), worker(4), None)
      } yield (took(again), polled.isDefined)
    }

    delivered shouldBe true
    offeredAgain shouldBe false
  }

  test("a failing copy frees exactly one slot on its worker") {
    val sch = task(Some(Replication(2)))

    val (offeredAgain, delivered) = withQueue(sch) { q =>
      for {
        _ <- q.askForWork(LauncherName("w1"), worker(16), None)
        _ <- q.askForWork(LauncherName("w1"), worker(12), None)
        _ <- q.taskFailed(
          sch,
          LauncherName("w1"),
          new RuntimeException("boom")
        )
        polled <- q.pollResult(proxyAddress)
        again <- q.askForWork(LauncherName("w2"), worker(4), None)
      } yield (took(again), polled.isDefined)
    }

    delivered shouldBe false
    offeredAgain shouldBe true
  }

  test("every copy lands on a worker sharing the first one's attribute") {
    val sch = task(Some(Replication(4, zone)))

    val (sameZone, otherZone, noZone) = withQueue(sch) { q =>
      for {
        _ <- q.askForWork(LauncherName("w1"), workerIn(4, "eu-west-1a"), None)
        same <- q.askForWork(
          LauncherName("w2"),
          workerIn(4, "eu-west-1a"),
          None
        )
        other <- q.askForWork(
          LauncherName("w3"),
          workerIn(4, "eu-west-1b"),
          None
        )
        bare <- q.askForWork(LauncherName("w4"), worker(4), None)
      } yield (took(same), took(other), took(bare))
    }

    sameZone shouldBe true
    otherZone shouldBe false
    noZone shouldBe false
  }

  test("a worker without the placement attribute never starts the task") {
    val sch = task(Some(Replication(2, zone)))

    withQueue(sch) { q =>
      q.askForWork(LauncherName("w1"), worker(4), None).map(took)
    } shouldBe false
  }

  test("the pinned attribute is released once every copy is gone") {
    val sch = task(Some(Replication(2, zone)))

    val (inA, inB) = withQueue(sch) { q =>
      for {
        a <- q.askForWork(LauncherName("w1"), workerIn(4, "eu-west-1a"), None)
        _ <- q.handleLauncherStopped(
          LauncherName("w1"),
          QueueImpl.LauncherStopReason.SelfReportedByThisProcess
        )
        b <- q.askForWork(LauncherName("w2"), workerIn(4, "eu-west-1b"), None)
      } yield (took(a), took(b))
    }

    inA shouldBe true
    inB shouldBe true
  }

  test("the first copy to succeed completes the caller") {
    val sch = task(Some(Replication(3)))

    val (delivered, offeredAfter) = withQueue(sch) { q =>
      for {
        _ <- q.askForWork(LauncherName("w1"), worker(4), None)
        _ <- q.askForWork(LauncherName("w2"), worker(4), None)
        _ <- q.taskSuccess(
          sch,
          LauncherName("w1"),
          result,
          ElapsedTimeNanoSeconds(1L),
          ResourceAllocated(4, 100, 0, Nil, None)
        )
        polled <- q.pollResult(proxyAddress)
        late <- q.askForWork(LauncherName("w3"), worker(4), None)
      } yield (polled.isDefined, took(late))
    }

    delivered shouldBe true
    offeredAfter shouldBe false
  }

  test("a failing copy does not complete the caller while a sibling runs") {
    val sch = task(Some(Replication(3)))

    val delivered = withQueue(sch) { q =>
      for {
        _ <- q.askForWork(LauncherName("w1"), worker(4), None)
        _ <- q.askForWork(LauncherName("w2"), worker(4), None)
        _ <- q.taskFailed(
          sch,
          LauncherName("w1"),
          new RuntimeException("boom")
        )
        polled <- q.pollResult(proxyAddress)
      } yield polled
    }

    delivered shouldBe None
  }

  test("the last copy to fail completes the caller with its failure") {
    val sch = task(Some(Replication(2)))

    val delivered = withQueue(sch) { q =>
      for {
        _ <- q.askForWork(LauncherName("w1"), worker(4), None)
        _ <- q.askForWork(LauncherName("w2"), worker(4), None)
        _ <- q.taskFailed(
          sch,
          LauncherName("w1"),
          new RuntimeException("first boom")
        )
        _ <- q.taskFailed(
          sch,
          LauncherName("w2"),
          new RuntimeException("last boom")
        )
        polled <- q.pollResult(proxyAddress)
      } yield polled
    }

    delivered.map {
      case QueueImpl.ProxyResultFailure(cause) => cause.getMessage
      case other                               => s"unexpected: $other"
    } shouldBe Some("last boom")
  }

  test("a success after a sibling failed still completes the caller") {
    val sch = task(Some(Replication(2)))

    val delivered = withQueue(sch) { q =>
      for {
        _ <- q.askForWork(LauncherName("w1"), worker(4), None)
        _ <- q.askForWork(LauncherName("w2"), worker(4), None)
        _ <- q.taskFailed(
          sch,
          LauncherName("w1"),
          new RuntimeException("boom")
        )
        _ <- q.taskSuccess(
          sch,
          LauncherName("w2"),
          result,
          ElapsedTimeNanoSeconds(1L),
          ResourceAllocated(4, 100, 0, Nil, None)
        )
        polled <- q.pollResult(proxyAddress)
      } yield polled
    }

    delivered.map {
      case QueueImpl.ProxyResultSuccess(_, _) => "success"
      case other                              => s"unexpected: $other"
    } shouldBe Some("success")
  }

  test("a lone copy that fails completes the caller immediately") {
    val sch = task(Some(Replication(3)))

    val delivered = withQueue(sch) { q =>
      for {
        _ <- q.askForWork(LauncherName("w1"), worker(4), None)
        _ <- q.taskFailed(
          sch,
          LauncherName("w1"),
          new RuntimeException("boom")
        )
        polled <- q.pollResult(proxyAddress)
      } yield polled
    }

    delivered.map {
      case QueueImpl.ProxyResultFailure(cause) => cause.getMessage
      case other                               => s"unexpected: $other"
    } shouldBe Some("boom")
  }

  test("a later copy does not deliver a second result") {
    val sch = task(Some(Replication(2)))

    val (first, second) = withQueue(sch) { q =>
      for {
        _ <- q.askForWork(LauncherName("w1"), worker(4), None)
        _ <- q.askForWork(LauncherName("w2"), worker(4), None)
        _ <- q.taskSuccess(
          sch,
          LauncherName("w1"),
          result,
          ElapsedTimeNanoSeconds(1L),
          ResourceAllocated(4, 100, 0, Nil, None)
        )
        a <- q.pollResult(proxyAddress)
        _ <- q.taskSuccess(
          sch,
          LauncherName("w2"),
          result,
          ElapsedTimeNanoSeconds(1L),
          ResourceAllocated(4, 100, 0, Nil, None)
        )
        b <- q.pollResult(proxyAddress)
      } yield (a.isDefined, b.isDefined)
    }

    first shouldBe true
    second shouldBe false
  }

  test("a copy that fails after a sibling succeeded does not fail the caller") {
    val sch = task(Some(Replication(2)))

    val (delivered, afterFailure) = withQueue(sch) { q =>
      for {
        _ <- q.askForWork(LauncherName("w1"), worker(4), None)
        _ <- q.askForWork(LauncherName("w2"), worker(4), None)
        _ <- q.taskSuccess(
          sch,
          LauncherName("w1"),
          result,
          ElapsedTimeNanoSeconds(1L),
          ResourceAllocated(4, 100, 0, Nil, None)
        )
        a <- q.pollResult(proxyAddress)
        _ <- q.taskFailed(
          sch,
          LauncherName("w2"),
          new RuntimeException("late boom")
        )
        b <- q.pollResult(proxyAddress)
      } yield (a.isDefined, b)
    }

    delivered shouldBe true
    afterFailure shouldBe None
  }

  test("losing one copy leaves the survivors running") {
    val sch = task(Some(Replication(3)))

    val (offeredAgain, delivered) = withQueue(sch) { q =>
      for {
        _ <- q.askForWork(LauncherName("w1"), worker(4), None)
        _ <- q.askForWork(LauncherName("w2"), worker(4), None)
        _ <- q.handleLauncherStopped(
          LauncherName("w1"),
          QueueImpl.LauncherStopReason.SelfReportedByThisProcess
        )
        again <- q.askForWork(LauncherName("w3"), worker(4), None)
        _ <- q.taskSuccess(
          sch,
          LauncherName("w2"),
          result,
          ElapsedTimeNanoSeconds(1L),
          ResourceAllocated(4, 100, 0, Nil, None)
        )
        polled <- q.pollResult(proxyAddress)
      } yield (took(again), polled.isDefined)
    }

    offeredAgain shouldBe true
    delivered shouldBe true
  }

  test("losing the last copy puts the task back in the queue") {
    val sch = task(Some(Replication(2)))

    val (queuedAfter, offeredAgain) = withQueue(sch) { q =>
      for {
        _ <- q.askForWork(LauncherName("w1"), worker(4), None)
        _ <- q.handleLauncherStopped(
          LauncherName("w1"),
          QueueImpl.LauncherStopReason.SelfReportedByThisProcess
        )
        again <- q.askForWork(LauncherName("w2"), worker(4), None)
      } yield (true, took(again))
    }

    queuedAfter shouldBe true
    offeredAgain shouldBe true
  }

  test("every copy is deducted from the node its own launcher runs on") {
    val w1 = LauncherName("w1")
    val w2 = LauncherName("w2")
    val total = ResourceAvailable(8, 8000, 0, Nil, None)
    val allocated = VersionedResourceAllocated(
      cv,
      ResourceAllocated(3, 100, 0, Nil, None)
    )
    val sch = task(Some(Replication(2)), cpu = 3)

    val state = QueueImpl.State(
      queuedTasks = Map.empty,
      scheduledTasks = Map(
        QueueImpl.project(sch) -> QueueImpl.ScheduledTask(
          sch = sch,
          dispatches = List(
            QueueImpl.Dispatch(w1, allocated),
            QueueImpl.Dispatch(w2, allocated)
          ),
          proxies = Nil,
          placementValue = None,
          resultDelivered = false
        )
      ),
      knownLaunchers = Map(
        w1 -> Some(Node(RunningJobId("node-1"), total, w1)),
        w2 -> Some(Node(RunningJobId("node-2"), total, w2))
      ),
      counters = Map.empty,
      nodes = tasks.elastic.NodeRegistryState.State(
        running = Map(
          RunningJobId("node-1") -> total,
          RunningJobId("node-2") -> total
        ),
        pending = Map.empty,
        cumulativeRequested = 0,
        inFlightRequests = Nil
      ),
      completedResults = Map.empty,
      mainProcesses = Set.empty
    )

    state.freeCapacityOfRunningNodes.map(_.cpu).sorted shouldBe List(5, 5)
  }

  test("a failure with resubmit keeps the survivors instead of requeueing") {
    val resubmitting = tasks.util.config.parse(() =>
      org.ekrich.config.ConfigFactory
        .parseString("tasks.resubmitFailedTask = true")
        .withFallback(org.ekrich.config.ConfigFactory.load())
    )

    val sch = task(Some(Replication(2)))

    val (delivered, offeredAgain) = LocalMessenger.make
      .flatMap { messenger =>
        QueueImpl.initRef(
          cache = null,
          messenger = messenger,
          shutdownNode = None,
          decideNewNode = None,
          createNode = None,
          convertRunningToPending = None,
          unmanagedResource = ResourceAvailable.empty,
          meterProvider = org.typelevel.otel4s.metrics.MeterProvider.noop[IO],
          mainProcessSession = None
        )(resubmitting)
      }
      .use { q =>
        for {
          _ <- q.scheduleTask(sch)
          _ <- q.askForWork(LauncherName("w1"), worker(4), None)
          _ <- q.askForWork(LauncherName("w2"), worker(4), None)
          _ <- q.taskFailed(
            sch,
            LauncherName("w1"),
            new RuntimeException("boom")
          )
          polled <- q.pollResult(proxyAddress)
          again <- q.askForWork(LauncherName("w3"), worker(4), None)
        } yield (polled.isDefined, took(again))
      }
      .unsafeRunSync()

    delivered shouldBe false
    offeredAgain shouldBe true
  }
}
