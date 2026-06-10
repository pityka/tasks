/*
 * The MIT License
 *
 * Copyright (c) 2026 Istvan Bartha
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
 */

package tasks

import cats.effect.IO
import cats.effect.std.Console
import cats.effect.unsafe.implicits.global
import org.ekrich.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.otel4s.context.LocalProvider
import org.typelevel.otel4s.sdk.common.Diagnostic
import org.typelevel.otel4s.sdk.context.{Context, LocalContextProvider}
import org.typelevel.otel4s.sdk.metrics.data.{
  MetricData,
  MetricPoints,
  PointData
}
import org.typelevel.otel4s.sdk.testkit.metrics.MetricsTestkit
import tasks.queue.{HashedTaskDescription, QueueImpl, QueueMetrics, TaskId}

class QueueMetricsTest extends AnyFunSuite with Matchers {

  // Implicits the testkit needs (Async[IO] is built-in).
  // Args are passed via implicit resolution so the source works on both
  // Scala 2.13 (positional) and Scala 3 (using) without `using` keyword.
  private implicit val diagnostic: Diagnostic[IO] =
    Diagnostic.fromConsole[IO]
  private implicit val localProvider: LocalContextProvider[IO] =
    LocalProvider.fromLiftIO[IO, Context]

  private val testConfig =
    tasks.util.config.parse(() =>
      ConfigFactory.load(ConfigFactory.parseString(""))
    )

  private def collect(
      stateSnapshot: IO[QueueImpl.State] = IO.pure(QueueImpl.State.empty)
  )(
      body: QueueMetrics => IO[Unit]
  ): List[MetricData] =
    MetricsTestkit
      .inMemory[IO]()
      .use { testkit =>
        QueueMetrics
          .make(testkit.meterProvider, stateSnapshot)(testConfig)
          .use { qm =>
            // collectMetrics must run inside the QueueMetrics resource scope:
            // observable gauge callbacks are unregistered when the resource closes.
            body(qm) *> testkit.collectMetrics
          }
      }
      .unsafeRunSync()

  private def find(metrics: List[MetricData], name: String): MetricData =
    metrics
      .find(_.name == name)
      .getOrElse(
        fail(
          s"metric $name not found; got: ${metrics.map(_.name).mkString(", ")}"
        )
      )

  private def sumPointsByTask(
      m: MetricData,
      taskId: String,
      version: String
  ): Long =
    m.data match {
      case s: MetricPoints.Sum =>
        s.points.toVector.collect {
          case p: PointData.LongNumber
              if attr(p.attributes, QueueMetrics.idKey) == Some(taskId) &&
                attr(p.attributes, QueueMetrics.versionKey) == Some(version) =>
            p.value
        }.sum
      case _ => fail(s"$m is not a Sum")
    }

  private def attr(
      attrs: org.typelevel.otel4s.Attributes,
      key: String
  ): Option[String] = attrs.get[String](key).map(_.value)

  private val descA = HashedTaskDescription(TaskId("alignment", 3), "hash-a")
  private val descB = HashedTaskDescription(TaskId("variant_call", 1), "hash-b")

  test("counters increment with paired (task.id, task.version) labels") {
    val metrics = collect() { qm =>
      for {
        _ <- qm.onTaskDone(descA, elapsedNanos = 5_000_000_000L)
        _ <- qm.onTaskDone(descA, elapsedNanos = 7_000_000_000L)
        _ <- qm.onTaskFailed(descA)
        _ <- qm.onCacheHit(descA)
        _ <- qm.onTaskDone(descB, elapsedNanos = 1_000_000_000L)
      } yield ()
    }

    sumPointsByTask(
      find(metrics, "tasks.completed.count"),
      "alignment",
      "3"
    ) shouldBe 2L
    sumPointsByTask(
      find(metrics, "tasks.completed.count"),
      "variant_call",
      "1"
    ) shouldBe 1L
    sumPointsByTask(
      find(metrics, "tasks.failed.count"),
      "alignment",
      "3"
    ) shouldBe 1L
    sumPointsByTask(
      find(metrics, "tasks.cache_hit.count"),
      "alignment",
      "3"
    ) shouldBe 1L
  }

  test(
    "execution.duration histogram records seconds with buckets [1,10,60,600,3600]"
  ) {
    val metrics = collect() { qm =>
      qm.onTaskDone(descA, elapsedNanos = 5_000_000_000L) *>
        qm.onTaskDone(descA, elapsedNanos = 30_000_000_000L)
    }

    val hist = find(metrics, "tasks.execution.duration")
    hist.unit shouldBe Some("s")
    hist.data match {
      case h: MetricPoints.Histogram =>
        val pt = h.points.toVector
          .collectFirst {
            case p: PointData.Histogram
                if attr(p.attributes, QueueMetrics.idKey).contains(
                  "alignment"
                ) =>
              p
          }
          .getOrElse(fail("no histogram point for alignment"))
        pt.boundaries.boundaries shouldBe Vector(1.0, 10.0, 60.0, 600.0, 3600.0)
        pt.stats.map(_.count) shouldBe Some(2L)
        // 5s in bucket le=10, 30s in bucket le=60. sum = 35s.
        pt.stats.map(_.sum) shouldBe Some(35.0)
      case other => fail(s"expected histogram, got $other")
    }
  }

  test(
    "queue.wait_time records elapsed between onEnqueued and onTaskScheduled"
  ) {
    val metrics = collect() { qm =>
      for {
        _ <- qm.onEnqueued(descA)
        _ <- IO.sleep(scala.concurrent.duration.DurationInt(50).millis)
        _ <- qm.onTaskScheduled(descA)
      } yield ()
    }

    find(metrics, "tasks.queue.wait_time").data match {
      case h: MetricPoints.Histogram =>
        val pt = h.points.toVector.head.asInstanceOf[PointData.Histogram]
        pt.stats.map(_.count) shouldBe Some(1L)
        // ~50ms; permissive on timing (between 0.01s and 1s)
        pt.stats.map(_.sum).exists(s => s > 0.01 && s < 1.0) shouldBe true
        pt.boundaries.boundaries shouldBe Vector(0.1, 1.0, 10.0, 60.0, 600.0)
      case other => fail(s"expected histogram, got $other")
    }
  }

  test("onTaskScheduled without prior onEnqueued is a no-op") {
    val metrics = collect() { qm => qm.onTaskScheduled(descA) }
    metrics.find(_.name == "tasks.queue.wait_time") shouldBe None
  }

  test("re-enqueue overwrites the wait-time clock") {
    val metrics = collect() { qm =>
      for {
        _ <- qm.onEnqueued(descA) // first enqueue, then immediately re-enqueue
        _ <- IO.sleep(scala.concurrent.duration.DurationInt(40).millis)
        _ <- qm.onEnqueued(descA) // clock resets
        _ <- IO.sleep(scala.concurrent.duration.DurationInt(40).millis)
        _ <- qm.onTaskScheduled(descA)
      } yield ()
    }

    find(metrics, "tasks.queue.wait_time").data match {
      case h: MetricPoints.Histogram =>
        val pt = h.points.toVector.head.asInstanceOf[PointData.Histogram]
        // Wait recorded from the second enqueue: ~40ms, not ~80ms.
        // Permissive band: < 100ms (would be ~80ms if clock didn't reset).
        pt.stats.map(_.sum).exists(_ < 0.1) shouldBe true
      case other => fail(s"expected histogram, got $other")
    }
  }

  test("cardinality cap folds overflow pairs to (_other, _other)") {
    val capOverrideConfig = tasks.util.config.parse(() =>
      ConfigFactory
        .parseString(
          "tasks.otel.maxSeries = 47"
        ) // pairCap = floor((47-2)/21)-1 = 1
        .withFallback(ConfigFactory.load())
    )

    val derivedCap = QueueMetrics.pairCap(capOverrideConfig.otelMaxSeries)
    derivedCap shouldBe 1

    val metrics = MetricsTestkit
      .inMemory[IO]()
      .use { testkit =>
        QueueMetrics
          .make(testkit.meterProvider, IO.pure(QueueImpl.State.empty))(
            capOverrideConfig
          )
          .use { qm =>
            // First pair gets admitted, second pair (and third) fold to _other.
            qm.onTaskDone(descA, elapsedNanos = 1_000_000_000L) *>
              qm.onTaskDone(descB, elapsedNanos = 1_000_000_000L) *>
              qm.onTaskDone(
                HashedTaskDescription(TaskId("third_task", 1), "h"),
                elapsedNanos = 1_000_000_000L
              ) *> testkit.collectMetrics
          }
      }
      .unsafeRunSync()

    val completed = find(metrics, "tasks.completed.count")
    // descA admitted: one point with (alignment, 3) value 1
    sumPointsByTask(completed, "alignment", "3") shouldBe 1L
    // descB and the third task fold into _other: combined value 2
    sumPointsByTask(
      completed,
      QueueMetrics.otherSentinel,
      QueueMetrics.otherSentinel
    ) shouldBe 2L
  }

  test("observable gauges read the current state snapshot at collect time") {
    // Synthesize a state with two queued tasks and one running task.
    val schA = stubScheduleTask(descA)
    val schB = stubScheduleTask(descB)
    val launcher = tasks.util.message.LauncherName("test-launcher")
    val allocated = tasks.shared.VersionedResourceAllocated(
      tasks.shared.CodeVersion("v1"),
      tasks.shared.ResourceAllocated(
        cpu = 4,
        memory = 1024,
        scratch = 0,
        gpu = Nil,
        image = None
      )
    )
    val state = QueueImpl.State(
      queuedTasks = Map(
        QueueImpl.ScheduleTaskEqualityProjection(schA.description) ->
          ((schA, Nil)),
        QueueImpl.ScheduleTaskEqualityProjection(schB.description) ->
          ((schB, Nil))
      ),
      scheduledTasks = Map(
        QueueImpl.ScheduleTaskEqualityProjection(schA.description) ->
          ((launcher, allocated, Nil, schA))
      ),
      knownLaunchers = Map.empty,
      counters = Map.empty,
      nodes = tasks.elastic.NodeRegistryState.State.empty
    )

    val metrics = collect(IO.pure(state))(_ => IO.unit)

    val queued = find(metrics, "tasks.queued.count").data match {
      case g: MetricPoints.Gauge => g.points.toVector
      case _                     => fail("expected gauge")
    }
    queued.size shouldBe 2

    val running = find(metrics, "tasks.running.count").data match {
      case g: MetricPoints.Gauge => g.points.toVector
      case _                     => fail("expected gauge")
    }
    running.size shouldBe 1

    val cpuTotal = find(metrics, "tasks.resources.allocated.cpu").data match {
      case g: MetricPoints.Gauge =>
        g.points.toVector.head.asInstanceOf[PointData.LongNumber].value
      case _ => fail("expected gauge")
    }
    cpuTotal shouldBe 4L

    val memTotal =
      find(metrics, "tasks.resources.allocated.memory").data match {
        case g: MetricPoints.Gauge =>
          g.points.toVector.head.asInstanceOf[PointData.LongNumber].value
        case _ => fail("expected gauge")
      }
    memTotal shouldBe 1024L
  }

  test("node-registry gauges reflect running/pending/inflight and cumulative") {
    val shape = tasks.shared.ResourceAvailable(
      cpu = 2,
      memory = 256,
      scratch = 0,
      gpu = Nil,
      image = None
    )
    val node1 = tasks.util.message.Node(
      tasks.shared.RunningJobId("r1"),
      shape,
      tasks.util.message.LauncherName("l1")
    )
    val node2 = tasks.util.message.Node(
      tasks.shared.RunningJobId("r2"),
      shape,
      tasks.util.message.LauncherName("l2")
    )

    val nodeRegistry = tasks.elastic.NodeRegistryState.State.empty
      .update(tasks.elastic.NodeRegistryState.NodeRequested(shape))
      .update(
        tasks.elastic.NodeRegistryState.NodeIsPending(
          tasks.shared.PendingJobId("p1"),
          shape,
          shape
        )
      )
      .update(
        tasks.elastic.NodeRegistryState.NodeIsUp(
          node1,
          tasks.shared.PendingJobId("p1")
        )
      )
      .update(tasks.elastic.NodeRegistryState.NodeRequested(shape))
      .update(
        tasks.elastic.NodeRegistryState.NodeIsPending(
          tasks.shared.PendingJobId("p2"),
          shape,
          shape
        )
      )
      .update(
        tasks.elastic.NodeRegistryState
          .NodeIsUp(node2, tasks.shared.PendingJobId("p2"))
      )
      .update(tasks.elastic.NodeRegistryState.NodeRequested(shape))
      .update(
        tasks.elastic.NodeRegistryState.NodeIsPending(
          tasks.shared.PendingJobId("p3"),
          shape,
          shape
        )
      )
      .update(tasks.elastic.NodeRegistryState.NodeRequested(shape))
      .update(tasks.elastic.NodeRegistryState.NodeRequested(shape))

    nodeRegistry.running.size shouldBe 2
    nodeRegistry.pending.size shouldBe 1
    nodeRegistry.inFlightRequests.size shouldBe 2
    nodeRegistry.cumulativeRequested shouldBe 5

    val state = QueueImpl.State(
      queuedTasks = Map.empty,
      scheduledTasks = Map.empty,
      knownLaunchers = Map.empty,
      counters = Map.empty,
      nodes = nodeRegistry
    )

    val metrics = collect(IO.pure(state))(_ => IO.unit)

    def gaugeLong(name: String): Long =
      find(metrics, name).data match {
        case g: MetricPoints.Gauge =>
          g.points.toVector.head.asInstanceOf[PointData.LongNumber].value
        case other => fail(s"expected gauge for $name, got $other")
      }

    gaugeLong("tasks.nodes.running.count") shouldBe 2L
    gaugeLong("tasks.nodes.pending.count") shouldBe 1L
    gaugeLong("tasks.nodes.inflight.count") shouldBe 2L

    find(metrics, "tasks.nodes.cumulative_requested").data match {
      case s: MetricPoints.Sum =>
        s.points.toVector.head
          .asInstanceOf[PointData.LongNumber]
          .value shouldBe 5L
      case other =>
        fail(s"expected sum for tasks.nodes.cumulative_requested, got $other")
    }
  }

  // Constructs a ScheduleTask with only the description field meaningfully populated.
  // QueueMetrics' state-snapshot callbacks read description.taskId and the resource
  // allocation tuple on scheduled tasks; everything else is irrelevant for these
  // tests.
  private def stubScheduleTask(
      description: HashedTaskDescription
  ): tasks.util.message.MessageData.ScheduleTask = {
    val emptySpore = tasks.queue.Spore[AnyRef, AnyRef](
      fqcn = "",
      dependencies = Seq.empty
    )
    tasks.util.message.MessageData.ScheduleTask(
      description = description,
      inputDeserializer = emptySpore,
      outputSerializer = emptySpore,
      function = emptySpore,
      resource = tasks.shared.VersionedResourceRequest(
        tasks.shared.CodeVersion("v1"),
        cpu = 1,
        memory = 100,
        scratch = 0,
        gpu = 0,
        image = None
      ),
      input = tasks.util.message.MessageData.InputData(
        tasks.queue.Base64Data(""),
        noCache = false
      ),
      fileServicePrefix = tasks.fileservice.FileServicePrefix(Vector.empty),
      tryCache = true,
      priority = tasks.shared.Priority(0),
      labels = tasks.shared.Labels.empty,
      lineage = tasks.queue.TaskLineage.root,
      proxy = tasks.util.message.Address("test")
    )
  }
}
