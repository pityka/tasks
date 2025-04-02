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

object ExternalQueueTest extends TestHelpers {

  val testTask = Task[Input, Int]("externalQueueStateTask", 1) { _ => _ =>
    scribe.info("Hello from task")
    IO(1)
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
      tasks.elastic.maxNodes = 3
      
      """
    )
  }

  def run = Ref
    .of[IO, tasks.queue.QueueImpl.State](tasks.queue.QueueImpl.State.empty)
    .flatMap { queueStateRef =>
      val io1 = withTaskSystem(
        config = Some(testConfig2),
        s3Client = Resource.pure(None),
        elasticSupport = JvmGrid.make(Some(queueStateRef)).map(v => Some(v._2)),
        externalQueueState = Resource.pure(
          Some(tasks.util.Transaction.fromRef(queueStateRef))
        )
      ) { implicit ts =>
        import scala.concurrent.ExecutionContext.Implicits.global

        val f1 = testTask(Input(1))(ResourceRequest(1, 500))

        val f2 = f1.flatMap(_ => testTask(Input(2))(ResourceRequest(1, 500)))
        val f3 = testTask(Input(3))(ResourceRequest(1, 500))
        val f4 = IO.parSequenceN(4)(
          (10 to 20).toList
            .map(i => testTask(Input(i))(ResourceRequest(1, 500)))
        )
        val future = for {
          t1 <- f1
          t2 <- f2
          t3 <- f3
          t4 <- f4
        } yield t1 + t2 + t3 + t4.sum

        (future)

      }
      val io2 = withTaskSystem(
        config = Some(testConfig2),
        s3Client = Resource.pure(None),
        elasticSupport = JvmGrid.make(Some(queueStateRef)).map(v => Some(v._2)),
        externalQueueState = Resource.pure(
          Some(tasks.util.Transaction.fromRef(queueStateRef))
        )
      ) { implicit ts =>
        import scala.concurrent.ExecutionContext.Implicits.global

        val f4 = IO.parSequenceN(4)(
          (20 to 30).toList
            .map(i => testTask(Input(i))(ResourceRequest(1, 500)))
        )
        val future = for {
          t4 <- f4
        } yield t4.sum

        (future)

      }
      io1.flatMap(a => io2.map(b => (a, b))).map { case (a, b) =>
        a.toOption.get + b.toOption.get
      }
    }

}

class ExternalQueueTestSuite extends FunSuite with Matchers {

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Info))
    .replace()

  test("elastic node allocation should spawn nodes") {
    ExternalQueueTest.run.unsafeRunSync() should equal(25)

  }

}
