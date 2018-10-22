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

import org.scalatest._

import org.scalatest.Matchers

import scala.concurrent.Future
import tasks.circesupport._
import com.typesafe.config.ConfigFactory

object KubernetesTestSlave extends App {
  withTaskSystem { _ =>
    Thread.sleep(100000)
  }
}

object KubernetesTest extends TestHelpers {

  val testTask = AsyncTask[Input, Int]("kubernetestest", 1) {
    input => implicit computationEnvironment =>
      log.info("Hello from task")
      Future(1)
  }

  val testConfig2 = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=0
      hosts.hostname="192.168.99.1"
      tasks.elastic.engine = "tasks.elastic.kubernetes.K8SElasticSupport"
      tasks.elastic.queueCheckInterval = 3 seconds  
      tasks.addShutdownHook = false
      tasks.failuredetector.acceptable-heartbeat-pause = 10 s
      tasks.kubernetes.image = "alpine37-openjdk8-curl:latest"
      tasks.kubernetes.image-pull-policy = "Never"
      tasks.slave-main-class = "tasks.KubernetesTestSlave"
      """
    )
  }

  def run = {
    withTaskSystem(testConfig2) { implicit ts =>
      import scala.concurrent.ExecutionContext.Implicits.global

      val f1 = testTask(Input(1))(ResourceRequest(1, 500))

      val f2 = f1.flatMap(_ => testTask(Input(2))(ResourceRequest(1, 500)))
      val f3 = testTask(Input(3))(ResourceRequest(1, 500))
      val future = for {
        t1 <- f1
        t2 <- f2
        t3 <- f3
      } yield t1 + t2 + t3

      import scala.concurrent.duration._
      scala.concurrent.Await.result(future, atMost = 400000 seconds)

    }
  }

}

class KubernetesTestSuite extends FunSuite with Matchers {

  test("kubernetes allocation should spawn nodes") {
    (KubernetesTest.run.get: Int) should equal(3)

  }

}
