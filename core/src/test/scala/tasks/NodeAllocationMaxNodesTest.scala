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
import scala.util._
import cats.effect.IO

object NodeAllocationMaxNodesTest extends TestHelpers {

  val testTask = Task[Input, Int]("nodeallocationtest", 1) { _ => _ =>
    scribe.info("Hello from task")
    IO(1)
  }

  val testConfig2 = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=0
      tasks.elastic.engine = "tasks.JvmElasticSupport$$JvmGrid$$"
      tasks.elastic.queueCheckInterval = 3 seconds  
      tasks.addShutdownHook = false
      tasks.elastic.maxNodes = 0
      tasks.disableRemoting = false
      """
    )
  }

  def run = {
    withTaskSystem(testConfig2) { implicit ts =>
      val f1 = testTask(Input(1))(ResourceRequest(1, 500))
      val f2 = testTask(Input(2))(ResourceRequest(1, 500))
      val f3 = testTask(Input(3))(ResourceRequest(1, 500))
      val future = for {
        t1 <- f1
        t2 <- f2
        t3 <- f3
      } yield t1 + t2 + t3
      import scala.concurrent.duration._
      (future).timeout(30 seconds).attempt.map(_.toOption)

    }
  }

}

class NodeAllocationMaxNodesTestSuite extends FunSuite with Matchers {

  test("elastic node allocation should respect max node configuration") {
    NodeAllocationMaxNodesTest.run.unsafeRunSync().get shouldBe None

  }

}
