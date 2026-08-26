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

package tasks.queue

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import org.typelevel.otel4s.{Attribute, Attributes}
import org.typelevel.otel4s.metrics.{
  BucketBoundaries,
  Counter,
  Histogram,
  Meter,
  MeterProvider,
  ObservableMeasurement
}
import tasks.util.config.TasksConfig

private[tasks] final class QueueMetrics(
    completedCounter: Counter[IO, Long],
    failedCounter: Counter[IO, Long],
    cacheHitCounter: Counter[IO, Long],
    executionDuration: Histogram[IO, Double],
    queueWaitTime: Histogram[IO, Double],
    cap: Int,
    admittedPairs: Ref[IO, Set[(String, Int)]],
    enqueueTimestamps: Ref[IO, Map[HashedTaskDescription, Long]],
    overflowWarned: Ref[IO, Boolean],
    labelCap: Int,
    admittedLabels: Ref[IO, Set[String]],
    labelOverflowWarned: Ref[IO, Boolean]
) {
  import QueueMetrics.{idKey, versionKey, labelKey, otherSentinel}

  // Returns the (task.id, task.version) attribute pair, applying the cardinality cap.
  // First `cap` distinct (id, version) pairs are admitted; subsequent novel pairs
  // fold into ("_other", "_other"). Called from both recording paths and observable
  // gauge callbacks — gauges share the same admittance set so labels stay coherent.
  private[queue] def attrsFor(taskId: TaskId): IO[Attributes] = {
    val isOverflowMarker: Attributes => Boolean =
      _.get[String](idKey).exists(_.value == otherSentinel)
    admittedPairs
      .modify { admitted =>
        val pair = (taskId.id, taskId.version)
        if (admitted.contains(pair))
          (admitted, attrPair(taskId.id, taskId.version.toString))
        else if (admitted.size < cap)
          (admitted + pair, attrPair(taskId.id, taskId.version.toString))
        else
          (admitted, attrPair(otherSentinel, otherSentinel))
      }
      .flatTap { attrs =>
        if (isOverflowMarker(attrs))
          overflowWarned.getAndSet(true).flatMap {
            case true => IO.unit
            case false =>
              IO(
                scribe.warn(
                  "OTel cardinality cap reached; subsequent novel (task.id, task.version) pairs fold into \"_other\". Raise tasks.otel.maxSeries to admit more.",
                  scribe.data(
                    Map(
                      "cap" -> cap,
                      "first-overflow-task-id" -> taskId.id,
                      "first-overflow-task-version" -> taskId.version
                    )
                  )
                )
              )
          }
        else IO.unit
      }
  }

  private def attrPair(id: String, version: String): Attributes =
    Attributes(Attribute(idKey, id), Attribute(versionKey, version))

  // Admittance for the `tasks.node.label` attribute used by the by_label
  // gauges. Same pattern as attrsFor — overflow folds into `_other`, with a
  // single WARN the first time the cap is hit.
  private[queue] def labelAttrsFor(label: String): IO[Attributes] = {
    val isOverflowMarker: Attributes => Boolean =
      _.get[String](labelKey).exists(_.value == otherSentinel)
    admittedLabels
      .modify { admitted =>
        if (admitted.contains(label))
          (admitted, Attributes(Attribute(labelKey, label)))
        else if (admitted.size < labelCap)
          (admitted + label, Attributes(Attribute(labelKey, label)))
        else
          (admitted, Attributes(Attribute(labelKey, otherSentinel)))
      }
      .flatTap { attrs =>
        if (isOverflowMarker(attrs))
          labelOverflowWarned.getAndSet(true).flatMap {
            case true => IO.unit
            case false =>
              IO(
                scribe.warn(
                  "OTel node-label cardinality cap reached; subsequent novel labels fold into \"_other\". Raise tasks.otel.maxSeries to admit more.",
                  scribe.data(
                    Map(
                      "label-cap" -> labelCap,
                      "first-overflow-label" -> label
                    )
                  )
                )
              )
          }
        else IO.unit
      }
  }

  // Always (re)sets the enqueue timestamp. Called on both initial enqueue and
  // re-enqueue after launcher crash or task failure (with resubmitFailedTask).
  // Resetting on re-enqueue treats the second wait as a separate observation.
  def onEnqueued(description: HashedTaskDescription): IO[Unit] =
    enqueueTimestamps.update { ts =>
      ts.updated(description, System.nanoTime())
    }

  def onTaskScheduled(description: HashedTaskDescription): IO[Unit] =
    enqueueTimestamps
      .modify { ts =>
        ts.get(description) match {
          case Some(start) => (ts - description, Some(start))
          case None        => (ts, None)
        }
      }
      .flatMap {
        case None => IO.unit
        case Some(startNanos) =>
          val elapsedSeconds = (System.nanoTime() - startNanos) / 1e9
          attrsFor(description.taskId).flatMap { attrs =>
            queueWaitTime.record(elapsedSeconds, attrs)
          }
      }

  def onTaskDone(
      description: HashedTaskDescription,
      elapsedNanos: Long
  ): IO[Unit] =
    attrsFor(description.taskId).flatMap { attrs =>
      completedCounter.inc(attrs) *> executionDuration.record(
        elapsedNanos / 1e9,
        attrs
      )
    }

  def onTaskFailed(description: HashedTaskDescription): IO[Unit] =
    attrsFor(description.taskId).flatMap(failedCounter.inc(_))

  def onCacheHit(description: HashedTaskDescription): IO[Unit] =
    attrsFor(description.taskId).flatMap(cacheHitCounter.inc(_))
}

private[tasks] object QueueMetrics {

  val idKey = "task.id"
  val versionKey = "task.version"
  val labelKey = "tasks.node.label"
  val otherSentinel = "_other"

  private val seriesPerTaskPair = 21
  private val fixedSeries = 6

  def pairCap(maxSeries: Int): Int =
    math.max(1, (maxSeries - fixedSeries) / seriesPerTaskPair - 1)

  /** Bound on distinct node labels admitted into label-keyed gauges. Carved out
    * of the same `tasks.otel.maxSeries` budget; novel labels beyond this cap
    * fold into the [[otherSentinel]] series. ~2% of total series at default
    * config (5000 → 100), which is plenty for realistic label schemes (cloud
    * region, queue name, instance class, …).
    */
  def labelCap(maxSeries: Int): Int = math.max(1, maxSeries / 50)

  val executionDurationBuckets: BucketBoundaries =
    BucketBoundaries(1.0, 10.0, 60.0, 600.0, 3600.0)

  val queueWaitTimeBuckets: BucketBoundaries =
    BucketBoundaries(0.1, 1.0, 10.0, 60.0, 600.0)

  private final case class Binding(
      observable: ObservableMeasurement[IO, Long],
      record: QueueImpl.State => IO[Unit]
  )

  private def sequenceRecords[A](
      values: Vector[A]
  )(f: A => IO[Unit]): IO[Unit] =
    values.foldLeft(IO.unit)((acc, a) => acc *> f(a))

  private def scalarGauge(
      meter: Meter[IO],
      name: String,
      description: String,
      unit: Option[String],
      select: QueueImpl.State => Long
  ): Resource[IO, Binding] = {
    val base = meter.observableGauge[Long](name).withDescription(description)
    val withUnit = unit.fold(base)(u => base.withUnit(u))
    Resource
      .eval(withUnit.createObserver)
      .map(obs => Binding(obs, st => obs.record(select(st))))
  }

  def make(
      meterProvider: MeterProvider[IO],
      stateSnapshot: IO[QueueImpl.State]
  )(implicit config: TasksConfig): Resource[IO, QueueMetrics] = {
    val cap = pairCap(config.otelMaxSeries)
    val labelsCap = labelCap(config.otelMaxSeries)

    for {
      meter <- Resource.eval(meterProvider.get("tasks-core"))
      completed <- Resource.eval(
        meter
          .counter[Long]("tasks.completed.count")
          .withDescription("Cumulative successful task completions.")
          .create
      )
      failed <- Resource.eval(
        meter
          .counter[Long]("tasks.failed.count")
          .withDescription("Cumulative task execution failures.")
          .create
      )
      cacheHit <- Resource.eval(
        meter
          .counter[Long]("tasks.cache_hit.count")
          .withDescription("Cumulative cache hits.")
          .create
      )
      execution <- Resource.eval(
        meter
          .histogram[Double]("tasks.execution.duration")
          .withDescription("Task execution time, seconds.")
          .withUnit("s")
          .withExplicitBucketBoundaries(executionDurationBuckets)
          .create
      )
      waitTime <- Resource.eval(
        meter
          .histogram[Double]("tasks.queue.wait_time")
          .withDescription("Time from enqueue to scheduled, seconds.")
          .withUnit("s")
          .withExplicitBucketBoundaries(queueWaitTimeBuckets)
          .create
      )
      admittedRef <- Resource.eval(Ref.of[IO, Set[(String, Int)]](Set.empty))
      enqueueRef <- Resource.eval(
        Ref.of[IO, Map[HashedTaskDescription, Long]](Map.empty)
      )
      overflowWarnedRef <- Resource.eval(Ref.of[IO, Boolean](false))
      admittedLabelsRef <- Resource.eval(Ref.of[IO, Set[String]](Set.empty))
      labelOverflowWarnedRef <- Resource.eval(Ref.of[IO, Boolean](false))

      // The QueueMetrics instance is needed for attrsFor — declared here so the
      // gauge callbacks can call it and share the same admittance set.
      qm = new QueueMetrics(
        completedCounter = completed,
        failedCounter = failed,
        cacheHitCounter = cacheHit,
        executionDuration = execution,
        queueWaitTime = waitTime,
        cap = cap,
        admittedPairs = admittedRef,
        enqueueTimestamps = enqueueRef,
        overflowWarned = overflowWarnedRef,
        labelCap = labelsCap,
        admittedLabels = admittedLabelsRef,
        labelOverflowWarned = labelOverflowWarnedRef
      )

      queuedCount <- Resource
        .eval(
          meter
            .observableGauge[Long]("tasks.queued.count")
            .withDescription(
              "Tasks currently queued, by task name and version."
            )
            .createObserver
        )
        .map { obs =>
          Binding(
            obs,
            st => {
              val byTask = st.queuedTasks.valuesIterator
                .map { case (sch, _) => sch.description.taskId }
                .toVector
                .groupBy(identity)
                .view
                .mapValues(_.size.toLong)
                .toVector
              sequenceRecords(byTask) { case (taskId, count) =>
                qm.attrsFor(taskId).flatMap(attrs => obs.record(count, attrs))
              }
            }
          )
        }

      runningCount <- Resource
        .eval(
          meter
            .observableGauge[Long]("tasks.running.count")
            .withDescription(
              "Tasks currently executing, by task name and version."
            )
            .createObserver
        )
        .map { obs =>
          Binding(
            obs,
            st => {
              val byTask = st.scheduledTasks.valuesIterator
                .map { case (_, _, _, sch) => sch.description.taskId }
                .toVector
                .groupBy(identity)
                .view
                .mapValues(_.size.toLong)
                .toVector
              sequenceRecords(byTask) { case (taskId, count) =>
                qm.attrsFor(taskId).flatMap(attrs => obs.record(count, attrs))
              }
            }
          )
        }

      allocatedCpu <- scalarGauge(
        meter,
        "tasks.resources.allocated.cpu",
        "Total CPU currently allocated to scheduled tasks.",
        None,
        st =>
          st.scheduledTasks.valuesIterator.map { case (_, alloc, _, _) =>
            alloc.cpuMemoryAllocated.cpu.toLong
          }.sum
      )

      allocatedMemory <- scalarGauge(
        meter,
        "tasks.resources.allocated.memory",
        "Total memory (MB) currently allocated to scheduled tasks.",
        Some("MB"),
        st =>
          st.scheduledTasks.valuesIterator.map { case (_, alloc, _, _) =>
            alloc.cpuMemoryAllocated.memory.toLong
          }.sum
      )

      nodesRunning <- scalarGauge(
        meter,
        "tasks.nodes.running.count",
        "Worker nodes currently running.",
        None,
        _.nodes.running.size.toLong
      )

      nodesPending <- scalarGauge(
        meter,
        "tasks.nodes.pending.count",
        "Worker nodes that have been allocated by the scheduler " +
          "but are not yet up.",
        None,
        _.nodes.pending.size.toLong
      )

      nodesInflight <- scalarGauge(
        meter,
        "tasks.nodes.inflight.count",
        "Node requests that have been pre-committed but not yet " +
          "resolved (still mid-spawn).",
        None,
        _.nodes.inFlightRequests.size.toLong
      )

      cumulativeRequested <- Resource
        .eval(
          meter
            .observableCounter[Long]("tasks.nodes.cumulative_requested")
            .withDescription(
              "Total node requests issued across the lifetime of this " +
                "task system (monotonic; counts failures too, gates " +
                "maxNodesCumulative)."
            )
            .createObserver
        )
        .map(obs =>
          Binding(obs, st => obs.record(st.nodes.cumulativeRequested.toLong))
        )

      nodeRegistry <- nodeRegistryGauges(meter)
      launcherAvailable <- launcherAvailableGauges(meter)
      queuedResources <- queuedResourceGauges(meter, qm)
      labels <- labelGauges(meter, qm)

      bindings = List(
        queuedCount,
        runningCount,
        allocatedCpu,
        allocatedMemory,
        nodesRunning,
        nodesPending,
        nodesInflight,
        cumulativeRequested
      ) ::: nodeRegistry ::: launcherAvailable ::: queuedResources ::: labels

      _ <- meter.batchCallback(
        stateSnapshot.flatMap(st =>
          sequenceRecords(bindings.toVector)(_.record(st))
        ),
        bindings.head.observable,
        bindings.tail.map(_.observable): _*
      )
    } yield qm
  }

  /** Walk a [[tasks.shared.NodeSelector]] and collect every label that appears
    * in a positive [[tasks.shared.NodeSelector.Has]] clause. Used by the
    * `tasks.queued.affinity_label` gauge.
    *
    * For `And` and `Or` this returns the union of the children's positive
    * labels: a task with `Or(Has(a), Has(b))` contributes a unit count to BOTH
    * a and b, since the task can be satisfied by either. This is the
    * over-counting compromise needed to keep the gauge label-keyed without
    * fanning out to power-sets.
    */
  private[queue] def positiveSelectorLabels(
      sel: tasks.shared.NodeSelector
  ): Set[String] = sel match {
    case tasks.shared.NodeSelector.Always     => Set.empty
    case tasks.shared.NodeSelector.Has(label) => Set(label)
    case tasks.shared.NodeSelector.Not(_)     => Set.empty
    case tasks.shared.NodeSelector.And(xs) =>
      xs.iterator.flatMap(positiveSelectorLabels).toSet
    case tasks.shared.NodeSelector.Or(xs) =>
      xs.iterator.flatMap(positiveSelectorLabels).toSet
  }

  private def labelGauges(
      meter: Meter[IO],
      qm: QueueMetrics
  ): Resource[IO, List[Binding]] = {

    def countByLabel(
        labelSources: QueueImpl.State => Iterator[Set[String]]
    ): QueueImpl.State => Map[String, Long] = { st =>
      val builder =
        collection.mutable.Map.empty[String, Long].withDefaultValue(0L)
      labelSources(st).foreach { labels =>
        labels.foreach(l => builder.update(l, builder(l) + 1L))
      }
      builder.toMap
    }

    def labelGauge(
        name: String,
        description: String,
        select: QueueImpl.State => Map[String, Long]
    ): Resource[IO, Binding] =
      Resource
        .eval(
          meter
            .observableGauge[Long](name)
            .withDescription(description)
            .createObserver
        )
        .map { obs =>
          Binding(
            obs,
            st => {
              val counts = select(st)
              // Resolve admittance first (some labels may fold into _other),
              // then SUM counts per resolved attribute set so a gauge point
              // labelled _other reflects the aggregate of all overflowed
              // labels rather than the last one recorded (OTel gauges are
              // last-wins per attribute set).
              counts.toVector
                .foldLeft(IO.pure(Map.empty[Attributes, Long])) {
                  case (accIO, (label, count)) =>
                    accIO.flatMap { acc =>
                      qm.labelAttrsFor(label).map { attrs =>
                        acc.updated(attrs, acc.getOrElse(attrs, 0L) + count)
                      }
                    }
                }
                .flatMap { byAttrs =>
                  sequenceRecords(byAttrs.toVector) { case (attrs, count) =>
                    obs.record(count, attrs)
                  }
                }
            }
          )
        }

    for {
      nodesRunningByLabel <- labelGauge(
        "tasks.nodes.running.by_label",
        "Worker nodes currently running, broken down by every label they advertise. " +
          "A node with N labels contributes one point per label.",
        countByLabel(_.nodes.running.valuesIterator.map(_.labels))
      )
      nodesPendingByLabel <- labelGauge(
        "tasks.nodes.pending.by_label",
        "Worker nodes allocated but not yet up, broken down by every label they " +
          "advertise. A node with N labels contributes one point per label.",
        countByLabel(_.nodes.pending.valuesIterator.map(_.labels))
      )
      queuedByAffinityLabel <- labelGauge(
        "tasks.queued.affinity_label",
        "Queued tasks whose ResourceRequest.nodeSelector references the given label " +
          "via a Has(...) clause. A task whose selector mentions K positive labels " +
          "contributes one point per label.",
        countByLabel { st =>
          st.queuedTasks.valuesIterator.map { case (sch, _) =>
            sch.resource.cpuMemoryRequest.nodeSelector
              .fold(Set.empty[String])(positiveSelectorLabels)
          }
        }
      )

      queuedWithSelector <- scalarGauge(
        meter,
        "tasks.queued.with_selector.count",
        "Queued tasks whose ResourceRequest.nodeSelector is set (any " +
          "affinity or avoidance constraint).",
        None,
        st =>
          st.queuedTasks.valuesIterator.count { case (sch, _) =>
            sch.resource.cpuMemoryRequest.nodeSelector.isDefined
          }.toLong
      )
    } yield List(
      nodesRunningByLabel,
      nodesPendingByLabel,
      queuedByAffinityLabel,
      queuedWithSelector
    )
  }

  private def sumResources(
      rs: Iterable[tasks.shared.ResourceAvailable]
  ): (Long, Long, Long, Long) = {
    var cpu = 0L
    var memory = 0L
    var scratch = 0L
    var gpu = 0L
    rs.foreach { r =>
      cpu += r.cpu.toLong
      memory += r.memory.toLong
      scratch += r.scratch.toLong
      gpu += r.gpu.size.toLong
    }
    (cpu, memory, scratch, gpu)
  }

  private def nodeRegistryGauges(
      meter: Meter[IO]
  ): Resource[IO, List[Binding]] = {
    def gauge(
        name: String,
        description: String,
        unit: Option[String],
        select: QueueImpl.State => Long
    ): Resource[IO, Binding] =
      scalarGauge(meter, name, description, unit, select)

    def fromRunning(st: QueueImpl.State) = sumResources(st.nodes.running.values)
    def fromPending(st: QueueImpl.State) = sumResources(st.nodes.pending.values)
    def fromInflight(st: QueueImpl.State) =
      sumResources(st.nodes.inFlightRequests)

    for {
      runningCpu <- gauge(
        "tasks.nodes.running.cpu",
        "Total CPU provisioned across running worker nodes.",
        None,
        fromRunning(_)._1
      )
      runningMemory <- gauge(
        "tasks.nodes.running.memory",
        "Total memory (MB) provisioned across running worker nodes.",
        Some("MB"),
        fromRunning(_)._2
      )
      runningScratch <- gauge(
        "tasks.nodes.running.scratch",
        "Total scratch space provisioned across running worker nodes.",
        None,
        fromRunning(_)._3
      )
      runningGpu <- gauge(
        "tasks.nodes.running.gpu",
        "Total GPU count provisioned across running worker nodes.",
        None,
        fromRunning(_)._4
      )

      pendingCpu <- gauge(
        "tasks.nodes.pending.cpu",
        "Total CPU on worker nodes allocated but not yet up.",
        None,
        fromPending(_)._1
      )
      pendingMemory <- gauge(
        "tasks.nodes.pending.memory",
        "Total memory (MB) on worker nodes allocated but not yet up.",
        Some("MB"),
        fromPending(_)._2
      )
      pendingScratch <- gauge(
        "tasks.nodes.pending.scratch",
        "Total scratch on worker nodes allocated but not yet up.",
        None,
        fromPending(_)._3
      )
      pendingGpu <- gauge(
        "tasks.nodes.pending.gpu",
        "Total GPU count on worker nodes allocated but not yet up.",
        None,
        fromPending(_)._4
      )

      inflightCpu <- gauge(
        "tasks.nodes.inflight.cpu",
        "Total CPU pre-committed for node requests still mid-spawn.",
        None,
        fromInflight(_)._1
      )
      inflightMemory <- gauge(
        "tasks.nodes.inflight.memory",
        "Total memory (MB) pre-committed for node requests still mid-spawn.",
        Some("MB"),
        fromInflight(_)._2
      )
      inflightScratch <- gauge(
        "tasks.nodes.inflight.scratch",
        "Total scratch pre-committed for node requests still mid-spawn.",
        None,
        fromInflight(_)._3
      )
      inflightGpu <- gauge(
        "tasks.nodes.inflight.gpu",
        "Total GPU count pre-committed for node requests still mid-spawn.",
        None,
        fromInflight(_)._4
      )
    } yield List(
      runningCpu,
      runningMemory,
      runningScratch,
      runningGpu,
      pendingCpu,
      pendingMemory,
      pendingScratch,
      pendingGpu,
      inflightCpu,
      inflightMemory,
      inflightScratch,
      inflightGpu
    )
  }

  private def launcherAvailableGauges(
      meter: Meter[IO]
  ): Resource[IO, List[Binding]] = {
    def allocatedSums(st: QueueImpl.State): (Long, Long, Long, Long) = {
      var cpu = 0L
      var memory = 0L
      var scratch = 0L
      var gpu = 0L
      st.scheduledTasks.valuesIterator.foreach { case (_, alloc, _, _) =>
        val a = alloc.cpuMemoryAllocated
        cpu += a.cpu.toLong
        memory += a.memory.toLong
        scratch += a.scratch.toLong
        gpu += a.gpu.size.toLong
      }
      (cpu, memory, scratch, gpu)
    }

    def availableFor(st: QueueImpl.State): (Long, Long, Long, Long) = {
      val (rCpu, rMem, rScr, rGpu) = sumResources(st.nodes.running.values)
      val (aCpu, aMem, aScr, aGpu) = allocatedSums(st)
      (rCpu - aCpu, rMem - aMem, rScr - aScr, rGpu - aGpu)
    }

    def gauge(
        name: String,
        description: String,
        unit: Option[String],
        select: QueueImpl.State => Long
    ): Resource[IO, Binding] =
      scalarGauge(meter, name, description, unit, select)

    for {
      availableCpu <- gauge(
        "tasks.launchers.available.cpu",
        "CPU currently free across launchers (running node total minus allocated).",
        None,
        availableFor(_)._1
      )
      availableMemory <- gauge(
        "tasks.launchers.available.memory",
        "Memory (MB) currently free across launchers (running node total minus allocated).",
        Some("MB"),
        availableFor(_)._2
      )
      availableScratch <- gauge(
        "tasks.launchers.available.scratch",
        "Scratch currently free across launchers (running node total minus allocated).",
        None,
        availableFor(_)._3
      )
      availableGpu <- gauge(
        "tasks.launchers.available.gpu",
        "GPU count currently free across launchers (running node total minus allocated).",
        None,
        availableFor(_)._4
      )
    } yield List(availableCpu, availableMemory, availableScratch, availableGpu)
  }

  private def queuedResourceGauges(
      meter: Meter[IO],
      qm: QueueMetrics
  ): Resource[IO, List[Binding]] = {

    def byTaskQueuedSums(
        st: QueueImpl.State
    ): Map[TaskId, (Long, Long, Long, Long)] = {
      val builder =
        collection.mutable.Map.empty[TaskId, (Long, Long, Long, Long)]
      st.queuedTasks.valuesIterator.foreach { case (sch, _) =>
        val taskId = sch.description.taskId
        val r = sch.resource
        val cpu = r.cpu._2.toLong
        val memory = r.memory.toLong
        val scratch = r.scratch.toLong
        val gpu = r.cpuMemoryRequest.gpu.toLong
        val (c, m, s, g) = builder.getOrElse(taskId, (0L, 0L, 0L, 0L))
        builder.update(taskId, (c + cpu, m + memory, s + scratch, g + gpu))
      }
      builder.toMap
    }

    def gauge(
        name: String,
        description: String,
        unit: Option[String],
        select: ((Long, Long, Long, Long)) => Long
    ): Resource[IO, Binding] = {
      val base = meter
        .observableGauge[Long](name)
        .withDescription(description)
      val withUnit = unit.fold(base)(u => base.withUnit(u))
      Resource
        .eval(withUnit.createObserver)
        .map { obs =>
          Binding(
            obs,
            st =>
              sequenceRecords(byTaskQueuedSums(st).toVector) {
                case (taskId, tuple) =>
                  qm.attrsFor(taskId)
                    .flatMap(attrs => obs.record(select(tuple), attrs))
              }
          )
        }
    }

    for {
      queuedCpu <- gauge(
        "tasks.queued.cpu",
        "Total max-CPU requested across currently queued tasks, by task name and version.",
        None,
        _._1
      )
      queuedMemory <- gauge(
        "tasks.queued.memory",
        "Total memory (MB) requested across currently queued tasks, by task name and version.",
        Some("MB"),
        _._2
      )
      queuedScratch <- gauge(
        "tasks.queued.scratch",
        "Total scratch requested across currently queued tasks, by task name and version.",
        None,
        _._3
      )
      queuedGpu <- gauge(
        "tasks.queued.gpu",
        "Total GPU count requested across currently queued tasks, by task name and version.",
        None,
        _._4
      )
    } yield List(queuedCpu, queuedMemory, queuedScratch, queuedGpu)
  }
}
