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

import scala.concurrent.duration._

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers
import org.ekrich.config.ConfigFactory

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global

import tasks.jsonitersupport._
import tasks.JvmElasticSupport.JvmGrid

object ImageElasticTest extends TestHelpers {

  // Returns the launcher (worker) address that ran the task — same trick as
  // NodeSelectorElasticTest, so we can prove two tasks landed on different
  // workers or reused the same one.
  val whichLauncher =
    Task[Input, String]("whichLauncher-image", 1) { _ => implicit ce =>
      IO.pure(launcherName.name)
    }

  def imageRequest(image: String)(implicit
      cv: tasks.shared.CodeVersion
  ): tasks.shared.VersionedResourceRequest =
    tasks.shared.VersionedResourceRequest(
      cv,
      tasks.shared.ResourceRequest(
        cpu = (1, 1),
        memory = 100,
        scratch = 0,
        gpu = 0,
        image = Some(image),
        nodeSelector = None
      )
    )

  def baseConfig(maxNodes: Int) = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      // hosts.numCPU = 0 forces every task onto an elastically-spawned worker
      // — the master node itself cannot run anything.
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
         |hosts.numCPU = 0
         |tasks.disableRemoting = false
         |tasks.addShutdownHook = false
         |tasks.elastic.maxNodes = $maxNodes
         |tasks.elastic.maxNodesCumulative = ${maxNodes * 4}
         |tasks.failuredetector.acceptable-heartbeat-pause = 10 s
         |""".stripMargin
    )
  }
}

class ImageElasticTestSuite extends FunSuite with Matchers {
  import ImageElasticTest._

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Info))
    .replace()

  test(
    "elastic workers advertise image and the master routes image-scoped tasks to them"
  ) {
    val program = withTaskSystem(
      baseConfig(maxNodes = 4),
      Resource.pure(None),
      JvmGrid
        .make(externalQueueState = None)
        .map(v => Some(v._2))
    ) { implicit ts =>
      // 1. image=A — JvmGrid spawns a worker whose hosts.image is set to A,
      //    so it advertises ResourceAvailable.image = Some("img-a").
      val a1: IO[String] = whichLauncher(Input(1))(imageRequest("img-a"))

      // 2. image=B — the existing image=A worker's canFulfillRequest fails on
      //    the image predicate, so the grid must spawn a SECOND worker, this
      //    one tagged image=B.
      val b1: IO[String] = whichLauncher(Input(2))(imageRequest("img-b"))

      // 3. image=A again — should be picked up by the worker from step 1
      //    (workers are reused when they fit), proving image is a stable,
      //    matched dimension across the worker's lifetime.
      val a2: IO[String] = whichLauncher(Input(3))(imageRequest("img-a"))

      for {
        a1n <- a1
        b1n <- b1
        a2n <- a2
      } yield (a1n, b1n, a2n)
    }.timeout(60.seconds)

    val (a1n, b1n, a2n) = program.unsafeRunSync().toOption.get

    a1n shouldBe a2n
    a1n should not be b1n
  }

  test(
    "task with an image that no worker can satisfy does not run within the timeout"
  ) {
    // maxNodes = 1: the first request pins the sole worker to image=A. A
    // subsequent request for image=B has no chance — the queue keeps looking
    // and the second future never completes.
    val program = withTaskSystem(
      baseConfig(maxNodes = 1),
      Resource.pure(None),
      JvmGrid
        .make(externalQueueState = None)
        .map(v => Some(v._2))
    ) { implicit ts =>
      // Pin the sole worker to image=A first.
      whichLauncher(Input(10))(imageRequest("img-a")).flatMap { _ =>
        whichLauncher(Input(11))(imageRequest("img-b"))
          .timeout(5.seconds)
          .attempt
          .map(_.toOption)
      }
    }

    program.unsafeRunSync().toOption.get shouldBe None
  }

}
