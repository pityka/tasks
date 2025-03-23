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

object ExternalQueueCompletionOfPreviousTaskTest extends TestHelpers {

  val testTask = Task[Input, Int]("externalQueueStateTask", 1) { _ => _ =>
    scribe.info("Hello from task")
    IO(1)
  }

  val tmp = tasks.util.TempFile.createTempFile(".temp")
  tmp.delete
  val testConfigNoCpu = {
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=0
      tasks.disableRemoting = false
      tasks.elastic.queueCheckInterval = 3 seconds  
      tasks.addShutdownHook = false
      tasks.failuredetector.acceptable-heartbeat-pause = 10 s
      tasks.elastic.maxNodes = 0
      
      """
    )
  }
  val testConfigCpu1 = {
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=1
      tasks.disableRemoting = false
      tasks.elastic.queueCheckInterval = 3 seconds  
      tasks.addShutdownHook = false
      tasks.failuredetector.acceptable-heartbeat-pause = 10 s
      tasks.elastic.maxNodes = 0
      
      """
    )
  }

  def run = Ref
    .of[IO, tasks.queue.QueueImpl.State](tasks.queue.QueueImpl.State.empty)
    .flatMap { queueStateRef =>
      val io1 = withTaskSystem(
        config = Some(testConfigNoCpu),
        s3Client = Resource.pure(None),
        elasticSupport = Resource.pure(None),
        externalQueueState = Resource.pure(
          Some(tasks.util.Transaction.fromRef(queueStateRef))
        )
      ) { implicit ts =>
        import scala.concurrent.ExecutionContext.Implicits.global

        val f1 = testTask(Input(1))(ResourceRequest(1, 500))

        f1.start.flatMap { _ =>
          import scala.concurrent.duration._
          def loop: IO[Unit] = IO.sleep(1 second) *> queueStateRef.get
            .map(_.queuedTasks.size >= 1)
            .flatMap(b => if (b) IO.unit else loop)
          loop
        }

      }
      val io2 = withTaskSystem(
        config = Some(testConfigCpu1),
        s3Client = Resource.pure(None),
        elasticSupport = Resource.pure(None),
        externalQueueState = Resource.pure(
          Some(tasks.util.Transaction.fromRef(queueStateRef))
        )
      ) { implicit ts =>
        import scala.concurrent.ExecutionContext.Implicits.global

        import scala.concurrent.duration._
        def loop: IO[Unit] = IO.sleep(1 second) *> queueStateRef.get
          .map(st => st.queuedTasks.size == 0 && st.scheduledTasks.size == 0)
          .flatMap(b => if (b) IO.unit else loop)
        loop

      }
      val io3 = withTaskSystem(
        config = Some(testConfigNoCpu),
        s3Client = Resource.pure(None),
        elasticSupport = Resource.pure(None),
        externalQueueState = Resource.pure(
          Some(tasks.util.Transaction.fromRef(queueStateRef))
        )
      ) { implicit ts =>
        import scala.concurrent.ExecutionContext.Implicits.global

        val f1 = testTask(Input(1))(ResourceRequest(1, 500))

        f1
      }
      (io1 *> io2 *> io3)
    }

}

class ExternalQueueCompletionOfPreviousTaskTestSuite
    extends FunSuite
    with Matchers {

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Info))
    .replace()

  test("elastic node allocation should spawn nodes") {
    ExternalQueueCompletionOfPreviousTaskTest.run.unsafeRunSync() should equal(
      Right(1)
    )

  }

}
