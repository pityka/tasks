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
  MeterProvider
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
    overflowWarned: Ref[IO, Boolean]
) {
  import QueueMetrics.{idKey, versionKey, otherSentinel}

  // Returns the (task.id, task.version) attribute pair, applying the cardinality cap.
  // First `cap` distinct (id, version) pairs are admitted; subsequent novel pairs
  // fold into ("_other", "_other"). Called from both recording paths and observable
  // gauge callbacks — gauges share the same admittance set so labels stay coherent.
  private[queue] def attrsFor(taskId: TaskId): IO[Attributes] = {
    val isOverflowMarker: Attributes => Boolean =
      _.get[String](idKey).exists(_.value == otherSentinel)
    admittedPairs.modify { admitted =>
      val pair = (taskId.id, taskId.version)
      if (admitted.contains(pair))
        (admitted, attrPair(taskId.id, taskId.version.toString))
      else if (admitted.size < cap)
        (admitted + pair, attrPair(taskId.id, taskId.version.toString))
      else
        (admitted, attrPair(otherSentinel, otherSentinel))
    }.flatTap { attrs =>
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

  // Always (re)sets the enqueue timestamp. Called on both initial enqueue and
  // re-enqueue after launcher crash or task failure (with resubmitFailedTask).
  // Resetting on re-enqueue treats the second wait as a separate observation.
  def onEnqueued(description: HashedTaskDescription): IO[Unit] =
    enqueueTimestamps.update { ts =>
      ts.updated(description, System.nanoTime())
    }

  def onTaskScheduled(description: HashedTaskDescription): IO[Unit] =
    enqueueTimestamps.modify { ts =>
      ts.get(description) match {
        case Some(start) => (ts - description, Some(start))
        case None        => (ts, None)
      }
    }.flatMap {
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
  val otherSentinel = "_other"

  private val seriesPerTaskPair = 21
  private val fixedSeries = 6

  def pairCap(maxSeries: Int): Int =
    math.max(1, (maxSeries - fixedSeries) / seriesPerTaskPair - 1)

  val executionDurationBuckets: BucketBoundaries =
    BucketBoundaries(1.0, 10.0, 60.0, 600.0, 3600.0)

  val queueWaitTimeBuckets: BucketBoundaries =
    BucketBoundaries(0.1, 1.0, 10.0, 60.0, 600.0)

  def make(
      meterProvider: MeterProvider[IO],
      stateSnapshot: IO[QueueImpl.State]
  )(implicit config: TasksConfig): Resource[IO, QueueMetrics] = {
    val cap = pairCap(config.otelMaxSeries)

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
        overflowWarned = overflowWarnedRef
      )

      _ <- meter
        .observableGauge[Long]("tasks.queued.count")
        .withDescription("Tasks currently queued, by task name and version.")
        .createWithCallback { obs =>
          stateSnapshot.flatMap { st =>
            val byTask = st.queuedTasks.valuesIterator
              .map { case (sch, _) => sch.description.taskId }
              .toVector
              .groupBy(identity)
              .view
              .mapValues(_.size.toLong)
              .toVector
            byTask.foldLeft(IO.unit) { case (acc, (taskId, count)) =>
              acc *> qm.attrsFor(taskId).flatMap(attrs =>
                obs.record(count, attrs)
              )
            }
          }
        }

      _ <- meter
        .observableGauge[Long]("tasks.running.count")
        .withDescription("Tasks currently executing, by task name and version.")
        .createWithCallback { obs =>
          stateSnapshot.flatMap { st =>
            val byTask = st.scheduledTasks.valuesIterator
              .map { case (_, _, _, sch) => sch.description.taskId }
              .toVector
              .groupBy(identity)
              .view
              .mapValues(_.size.toLong)
              .toVector
            byTask.foldLeft(IO.unit) { case (acc, (taskId, count)) =>
              acc *> qm.attrsFor(taskId).flatMap(attrs =>
                obs.record(count, attrs)
              )
            }
          }
        }

      _ <- meter
        .observableGauge[Long]("tasks.resources.allocated.cpu")
        .withDescription("Total CPU currently allocated to scheduled tasks.")
        .createWithCallback { obs =>
          stateSnapshot.flatMap { st =>
            val total =
              st.scheduledTasks.valuesIterator
                .map { case (_, alloc, _, _) =>
                  alloc.cpuMemoryAllocated.cpu.toLong
                }
                .sum
            obs.record(total)
          }
        }

      _ <- meter
        .observableGauge[Long]("tasks.resources.allocated.memory")
        .withDescription(
          "Total memory (MB) currently allocated to scheduled tasks."
        )
        .withUnit("MB")
        .createWithCallback { obs =>
          stateSnapshot.flatMap { st =>
            val total =
              st.scheduledTasks.valuesIterator
                .map { case (_, alloc, _, _) =>
                  alloc.cpuMemoryAllocated.memory.toLong
                }
                .sum
            obs.record(total)
          }
        }

      _ <- meter
        .observableGauge[Long]("tasks.nodes.running.count")
        .withDescription("Worker nodes currently running.")
        .createWithCallback { obs =>
          stateSnapshot.flatMap(st => obs.record(st.nodes.running.size.toLong))
        }

      _ <- meter
        .observableGauge[Long]("tasks.nodes.pending.count")
        .withDescription(
          "Worker nodes that have been allocated by the scheduler " +
            "but are not yet up."
        )
        .createWithCallback { obs =>
          stateSnapshot.flatMap(st => obs.record(st.nodes.pending.size.toLong))
        }

      _ <- meter
        .observableGauge[Long]("tasks.nodes.inflight.count")
        .withDescription(
          "Node requests that have been pre-committed but not yet " +
            "resolved (still mid-spawn)."
        )
        .createWithCallback { obs =>
          stateSnapshot.flatMap(st =>
            obs.record(st.nodes.inFlightRequests.size.toLong)
          )
        }

      _ <- meter
        .observableCounter[Long]("tasks.nodes.cumulative_requested")
        .withDescription(
          "Total node requests issued across the lifetime of this " +
            "task system (monotonic; counts failures too, gates " +
            "maxNodesCumulative)."
        )
        .createWithCallback { obs =>
          stateSnapshot.flatMap(st =>
            obs.record(st.nodes.cumulativeRequested.toLong)
          )
        }
    } yield qm
  }
}
