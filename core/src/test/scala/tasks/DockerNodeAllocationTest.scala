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
import com.typesafe.config.ConfigFactory
import cats.effect.IO

object DockerTestWorker extends App {
  withTaskSystem(ConfigFactory.parseString(
    "tasks.disableRemoting = false"
  )) { _ =>
    IO.never
  }.unsafeRunSync()
}

object DockerTest extends TestHelpers {

  val testTask = Task[Input, Int]("dockertest", 1) { _ => _ =>
    scribe.info("Hello from task")
    if (tasks.elastic.docker.DockerGetNodeName.getNodeName.hashCode % 2 == 0)
      System.exit(1)
    IO(1)
  }

  val testConfig2 = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=0
      tasks.disableRemoting = false
      tasks.elastic.engine = "tasks.elastic.docker.DockerElasticSupport"
      tasks.docker.contexts = [
        {
          context = default
          host = localhost
        }
        ]
      tasks.elastic.queueCheckInterval = 3 seconds  
      tasks.addShutdownHook = false
      tasks.failuredetector.acceptable-heartbeat-pause = 5 s
      tasks.worker-main-class = "tasks.DockerTestWorker"
      tasks.resubmitFailedTask = true
      
      tasks.elastic.maxNodesCumulative = 1000
      """
    )
  }

  def run = {
    withTaskSystem(testConfig2) { implicit ts =>
      val f1 = testTask(Input(1))(ResourceRequest(1, 500))

      val f2 = f1.flatMap(_ => testTask(Input(2))(ResourceRequest(1, 500)))
      val f3 = testTask(Input(3))(ResourceRequest(1, 500))
      val future = for {
        t1 <- f1
        t2 <- f2
        t3 <- f3
      } yield t1 + t2 + t3

      future

    }
  }

}

class DockerTestSuite extends FunSuite with Matchers {

  test("sh allocation should spawn nodes") {
    (DockerTest.run.unsafeRunSync().get: Int) should equal(3)

  }

}
