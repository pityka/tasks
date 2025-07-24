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

object CancellationTest extends TestHelpers {

  val cancelled = new java.util.concurrent.atomic.AtomicBoolean(false)
  val start = cats.effect.Deferred[IO, Boolean].unsafeRunSync()

  val testTask = Task[Input, Int]("cancellationtest", 1) { _ => _ =>
    scribe.info("Hello from task")
    start.complete(true) *>
      IO.never.onCancel(IO.blocking(cancelled.set(true))).map(_ => 1)
  }

  val testConfig2 = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=0
      tasks.disableRemoting = false
      tasks.elastic.queueCheckInterval = 3 seconds  
      tasks.addShutdownHook = false
      tasks.failuredetector.acceptable-heartbeat-pause = 10 s
      
      """
    )
  }

  def run = {
    JvmGrid.make(None).use { case (nodeControl, es) =>
      val io = withTaskSystem(
        testConfig2,
        Resource.pure(None),
        Resource.pure(Some(es))
      ) { implicit ts =>
        import scala.concurrent.ExecutionContext.Implicits.global

        val f1 = testTask(Input(1))(ResourceRequest(1, 500))

        val f2 = f1.flatMap(_ => testTask(Input(2))(ResourceRequest(1, 500)))
        val f3 = testTask(Input(3))(ResourceRequest(1, 500))
        val future = for {
          t1 <- f1
        } yield t1

        (future)

      }
      import scala.concurrent.duration._
      io.start
        .flatMap(fiber => start.get *> fiber.cancel)
        .flatMap(_ => nodeControl.list)
    }

  }

}

class CancellationTestSuite extends FunSuite with Matchers {

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Info))
    .replace()

  test("should cancel") {

    assert(CancellationTest.run.unsafeRunSync() == Nil)
    assert(CancellationTest.cancelled.get == true)

  }

}
