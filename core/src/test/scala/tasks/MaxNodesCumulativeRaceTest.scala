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

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers
import cats.effect.IO
import cats.effect.kernel.{Deferred, Ref, Resource}
import cats.effect.unsafe.implicits.global
import cats.effect.ExitCode

import org.ekrich.config.ConfigFactory

import tasks.jsonitersupport._
import tasks.elastic._
import tasks.shared._
import tasks.util.config.TasksConfig

import scala.concurrent.duration._

/** Regression test for the maxNodesCumulative race:
  *
  * `handleQueueStat` runs the check (`cumulativeRequested < maxNodesCumulative`)
  * inside a `ref.flatModify`, but historically returned the *unchanged* state and
  * deferred the actual `cumulativeRequested += 1` updates into the IO that runs
  * after the modify releases. Concurrent task submissions caused many parallel
  * invocations to each read the same stale `cumulativeRequested = 0`, each decide
  * to submit a node, and collectively blow past the configured cumulative cap.
  *
  * This test wires an [[ElasticSupport]] that:
  *   - counts every `requestOneNewJobFromJobScheduler` invocation,
  *   - never lets allocated nodes become live (returns `Left` so pending/running
  *     stays at 0 and only the cumulative counter matters),
  *   - adds a small delay inside the request to widen the race window.
  *
  * With the bug, the counter blows past `maxNodesCumulative`.
  * With the fix (state pre-committed inside `flatModify`), the counter is exactly
  * equal to `maxNodesCumulative`.
  */
object MaxNodesCumulativeRaceTest extends TestHelpers {

  val MaxNodesCumulative = 3

  // A task that just sleeps; we need many of these to enqueue in parallel so that
  // `handleQueueStat` is invoked many times in rapid succession.
  val sleepingTask = Task[Input, Int]("maxNodesCumulativeRaceTask", 1) { _ => _ =>
    IO.sleep(60.seconds).as(0)
  }

  class CountingCreateNode(counter: Ref[IO, Int]) extends CreateNode {
    def requestOneNewJobFromJobScheduler(
        requestSize: ResourceRequest
    )(implicit config: TasksConfig)
        : IO[Either[String, (PendingJobId, ResourceAvailable)]] =
      // Delay widens the race window: many concurrent callers all enter, increment
      // the (test) counter, but with the production fix none of them should make
      // it past the modify because cumulativeRequested is pre-committed.
      IO.sleep(50.millis) *>
        counter.update(_ + 1) *>
        IO.pure(Left("intentionally failing so pending/running stays at 0"))
  }

  class NoOpShutdown extends ShutdownNode with ShutdownSelfNode {
    def shutdownRunningNode(nodeName: RunningJobId): IO[Unit] = IO.unit
    def shutdownRunningNode(
        exitCode: Deferred[IO, ExitCode],
        nodeName: RunningJobId
    ): IO[Unit] = IO.unit
    def shutdownPendingNode(nodeName: PendingJobId): IO[Unit] = IO.unit
  }

  class CountingCreateNodeFactory(counter: Ref[IO, Int]) extends CreateNodeFactory {
    def apply(
        master: tasks.util.SimpleSocketAddress,
        masterPrefix: String,
        codeAddress: CodeAddress
    ) = new CountingCreateNode(counter)
  }

  object CountingGetNodeName extends GetNodeName {
    def getNodeName(config: TasksConfig) = IO.pure(RunningJobId(config.nodeName))
  }

  def elasticSupport(counter: Ref[IO, Int]): Resource[IO, Option[ElasticSupport]] =
    Resource.pure(
      Some(
        new ElasticSupport(
          hostConfig = None,
          shutdownFromNodeRegistry = new NoOpShutdown,
          shutdownFromWorker = new NoOpShutdown,
          createNodeFactory = new CountingCreateNodeFactory(counter),
          getNodeName = CountingGetNodeName
        )
      )
    )

  val testConfig2 = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU = 0
      tasks.elastic.queueCheckInterval = 200 milliseconds
      tasks.elastic.queueCheckInitialDelay = 100 milliseconds
      tasks.addShutdownHook = false
      tasks.elastic.maxNodes = 100
      tasks.elastic.maxNodesCumulative = $MaxNodesCumulative
      tasks.disableRemoting = false
      """
    )
  }

  def run: IO[Int] = {
    Ref.of[IO, Int](0).flatMap { counter =>
      withTaskSystem(
        testConfig2,
        Resource.pure(None),
        elasticSupport(counter)
      ) { implicit ts =>
        // Enqueue many tasks in parallel. With hosts.numCPU=0, none can run on
        // the master, so the queue stays full and the elastic scaler is pressured
        // to spawn nodes repeatedly.
        val n = 100
        import cats.syntax.all._
        val submit = (1 to n).toList.parTraverse { i =>
          sleepingTask(Input(i))(tasks.ResourceRequest(cpu=(1,1), memory=1,scratch=0,gpu=0)).attempt.void
        }.start

        for {
          fiber <- submit
          // Give the queue checker plenty of time to fire repeatedly.
          _ <- IO.sleep(5.seconds)
          c <- counter.get
          _ <- fiber.cancel
        } yield c
      }.map(_.toOption.get)
    }
  }
}

class MaxNodesCumulativeRaceTestSuite extends FunSuite with Matchers {

  test("maxNodesCumulative is respected under concurrent task submission") {
    val count = MaxNodesCumulativeRaceTest.run.unsafeRunSync()
    count shouldBe MaxNodesCumulativeRaceTest.MaxNodesCumulative
  }

}
