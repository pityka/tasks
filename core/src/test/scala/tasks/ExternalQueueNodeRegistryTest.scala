/*
 * The MIT License
 *
 * Copyright (c) 2018 Istvan Bartha
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
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import cats.effect.unsafe.implicits.global

import org.scalatest.matchers.should.Matchers

import tasks.jsonitersupport._
import org.ekrich.config.ConfigFactory
import cats.effect.IO
import tasks.JvmElasticSupport.JvmGrid
import cats.effect.kernel.Resource
import cats.effect.kernel.Ref
import cats.effect.kernel.Deferred
import scala.concurrent.duration._

object ExternalQueueNodeRegistryTest extends TestHelpers {

  val taskStarted = Deferred[IO, Unit].unsafeRunSync()
  val releaseTask = Deferred[IO, Unit].unsafeRunSync()

  val blockingTask =
    Task[Input, Int]("externalQueueNodeRegistry", 1) { _ => _ =>
      taskStarted.complete(()) *> releaseTask.get.map(_ => 1)
    }

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
      tasks.elastic.pendingNodeTimeout = 300 s
      """
    )
  }

  case class Observation(
      running: Int,
      pending: Int,
      launchersWithNode: Int
  )

  private def observeRegistry(
      ref: Ref[IO, tasks.queue.QueueImpl.State]
  ): IO[Observation] = {
    def read = ref.get.map { state =>
      Observation(
        running = state.nodes.running.size,
        pending = state.nodes.pending.size,
        launchersWithNode = state.knownLaunchers.count(_._2.isDefined)
      )
    }
    def loop(remaining: Int): IO[Observation] = read.flatMap { observation =>
      if (observation.running > 0 || remaining <= 0) IO.pure(observation)
      else IO.sleep(200.millis) *> loop(remaining - 1)
    }
    loop(40)
  }

  def run = Ref
    .of[IO, tasks.queue.QueueImpl.State](tasks.queue.QueueImpl.State.empty)
    .flatMap { queueStateRef =>
      JvmGrid.make(Some(queueStateRef)).use { case (_, elasticSupport) =>
        withTaskSystem(
          config = Some(externalQueueConfig),
          s3Client = Resource.pure(None),
          elasticSupport = Resource.pure(Some(elasticSupport)),
          externalQueueState = Resource.pure(
            Some(tasks.util.Transaction.fromRef(queueStateRef))
          )
        ) { implicit ts =>
          for {
            fiber <- blockingTask(Input(1))(ResourceRequest(1, 500)).start
            _ <- taskStarted.get
            observation <- observeRegistry(queueStateRef)
            _ <- releaseTask.complete(())
            result <- fiber.joinWithNever
          } yield (observation, result)
        }
      }
    }

}

class ExternalQueueNodeRegistryTestSuite extends FunSuite with Matchers {

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Info))
    .replace()

  test(
    "a worker node moves from pending to running in the registry when the queue state is external"
  ) {
    val (observation, result) = ExternalQueueNodeRegistryTest.run
      .unsafeRunTimed(120.seconds)
      .getOrElse(throw new RuntimeException("timeout"))
      .toOption
      .get

    result shouldBe 1
    observation.launchersWithNode shouldBe 1
    observation.running shouldBe 1
    observation.pending shouldBe 0
  }

}
