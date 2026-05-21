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

/** Regression test for the maxNodes race.
  *
  * `handleQueueStat` pre-commits `cumulativeRequested` inside the `flatModify`
  * (see `MaxNodesCumulativeRaceTest`), but `pending` is only updated
  * asynchronously, *after* `requestOneNewJobFromJobScheduler` returns
  * `Right(...)`. While a node-spawn call is in flight, subsequent `flatModify`
  * invocations still see `pending.size == 0`, so the
  * `maxNodes > running + pending` gate keeps admitting new requests. The only
  * remaining throttle is `maxNodesCumulative`, so the worker pool overshoots
  * `maxNodes` by up to `maxNodesCumulative - maxNodes`.
  *
  * This test:
  *   - sets `maxNodes = 2` and `maxNodesCumulative = 50`,
  *   - returns `Right(...)` from `requestOneNewJobFromJobScheduler` but only
  *     after a long sleep — long enough that no `NodeIsPending` event lands
  *     during the test window,
  *   - counts how many times the scheduler invoked
  *     `requestOneNewJobFromJobScheduler`.
  *
  * With the bug, the counter grows up to `maxNodesCumulative`. With the fix,
  * the counter is capped at `maxNodes`.
  */
object MaxNodesRaceTest extends TestHelpers {

  val MaxNodes = 2
  val MaxNodesCumulative = 50

  val sleepingTask = Task[Input, Int]("maxNodesRaceTask", 1) { _ => _ =>
    IO.sleep(60.seconds).as(0)
  }

  /** Records every invocation and stalls long enough that `NodeIsPending` never
    * lands during the test window — keeping `pending.size == 0` so the
    * `maxNodes` gate is exercised in the racy regime.
    */
  class CountingCreateNode(counter: Ref[IO, Int]) extends CreateNode {
    def requestOneNewJobFromJobScheduler(
        requestSize: ResourceRequest
    )(implicit
        config: TasksConfig
    ): IO[Either[String, (PendingJobId, ResourceAvailable)]] =
      counter.update(_ + 1) *>
        IO.sleep(60.seconds) *>
        IO.pure(
          Right(
            (
              PendingJobId("never-pending"),
              ResourceAvailable(
                cpu = 1,
                memory = 1,
                scratch = 0,
                gpu = Nil,
                image = None
              )
            )
          )
        )
  }

  class NoOpShutdown extends ShutdownNode with ShutdownSelfNode {
    def shutdownRunningNode(nodeName: RunningJobId): IO[Unit] = IO.unit
    def shutdownRunningNode(
        exitCode: Deferred[IO, ExitCode],
        nodeName: RunningJobId
    ): IO[Unit] = IO.unit
    def shutdownPendingNode(nodeName: PendingJobId): IO[Unit] = IO.unit
  }

  class CountingCreateNodeFactory(counter: Ref[IO, Int])
      extends CreateNodeFactory {
    def apply(
        master: tasks.util.SimpleSocketAddress,
        masterPrefix: String,
        codeAddress: CodeAddress
    ) = new CountingCreateNode(counter)
  }

  object CountingGetNodeName extends GetNodeName {
    def getNodeName(config: TasksConfig) =
      IO.pure(RunningJobId(config.nodeName))
  }

  def elasticSupport(
      counter: Ref[IO, Int]
  ): Resource[IO, Option[ElasticSupport]] =
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
      tasks.elastic.maxNodes = $MaxNodes
      tasks.elastic.maxNodesCumulative = $MaxNodesCumulative
      tasks.elastic.maxPending = $MaxNodesCumulative
      tasks.elastic.pendingNodeTimeout = 5 minutes
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
        val n = 100
        import cats.syntax.all._
        val submit = (1 to n).toList.parTraverse { i =>
          sleepingTask(Input(i))(
            tasks
              .ResourceRequest(cpu = (1, 1), memory = 1, scratch = 0, gpu = 0)
          ).attempt.void
        }.start

        for {
          fiber <- submit
          _ <- IO.sleep(5.seconds)
          c <- counter.get
          _ <- fiber.cancel
        } yield c
      }.map(_.toOption.get)
    }
  }
}

class MaxNodesRaceTestSuite extends FunSuite with Matchers {

  test("maxNodes is respected while node-spawn calls are in flight") {
    val count = MaxNodesRaceTest.run.unsafeRunSync()
    count shouldBe MaxNodesRaceTest.MaxNodes
  }

}
