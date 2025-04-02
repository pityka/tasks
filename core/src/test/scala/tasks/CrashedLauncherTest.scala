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
import cats.effect.kernel.Deferred

object CrashedLauncherTest extends TestHelpers {

  val launcher1Up = Deferred[IO, Boolean].unsafeRunSync()
  val launcher1Stopped = Deferred[IO, Boolean].unsafeRunSync()

  val testTask = Task[Input, Int]("CrashedLauncherTest", 1) { _ => _ =>
    scribe.info("Hello from task")
    for {
      _ <- IO { scribe.warn("Launcher 1 up") }
      _ <- launcher1Up.complete(true)
      _ <- IO { scribe.warn("Waiting for launcher 1 stopped") }
      _ <- launcher1Stopped.get
      _ <- IO { scribe.warn("Launcher1 stopped.") }
    } yield (1)
  }

  val testConfig2 = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      tasks.cache.enabled = false
      hosts.numCPU=0
      tasks.disableRemoting = false
      tasks.elastic.queueCheckInterval = 3 seconds  
      tasks.addShutdownHook = false
      tasks.failuredetector.acceptable-heartbeat-pause = 10 s
      
      """
    )
  }

  def run = {
    JvmGrid.make(None).use { case (nodeController, elasticSupport) =>
      withTaskSystem(
        testConfig2,
        Resource.pure(None),
        Resource.pure(Some(elasticSupport))
      ) { implicit ts =>
        import scala.concurrent.ExecutionContext.Implicits.global

        val stopFirstLauncher: IO[Boolean] = launcher1Up.get.flatMap { _ =>
          nodeController.list.flatMap { list =>
            scribe.info(s"Found list of nodes: $list")
            nodeController
              .stop(list.head)
              .flatMap(_ => launcher1Stopped.complete(true))
          }
        }

        val f1 = stopFirstLauncher.start
          .flatMap(_ => testTask(Input(1))(ResourceRequest(1, 500)))

        (f1)

      }
    }
  }

}

class CrashedLauncherTestSuite extends FunSuite with Matchers {

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Info))
    .replace()

  test("respawn node and requeue task after launcher is removed") {
    CrashedLauncherTest.run.unsafeRunSync().toOption.get should equal(1)

  }

}
