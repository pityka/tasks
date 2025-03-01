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
import com.typesafe.config.ConfigFactory

import tasks.jsonitersupport._
import cats.effect.IO

object FailingTasksTest extends TestHelpers {

  val sideEffect = scala.collection.mutable.ArrayBuffer[String]()

  override def testConfig = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=4
      tasks.cache.enabled = false
      
      """
    )
  }

  class TestException extends RuntimeException("expected exception thrown")

  val fail = Task[Input, String]("failingtasktest-fail", 1) { _ => _ =>
    sideEffect += "execution of fail task"
    val `true` = true
    if (`true`) {
      throw new TestException
    }
    IO.pure("fail")
  }

  val success = Task[Input, String]("failingtasktest-success", 1) { _ => _ =>
    sideEffect += "execution of success task"
    IO("succeeded")
  }

  def run: IO[Option[(String, String, String)]] = {
    withTaskSystem(testConfig) { implicit ts =>
      // import scala.concurrent.ExecutionContext.Implicits.global

      val f1 = fail(Input(1))(ResourceRequest(1, 500)).handleError {
        case _: TestException => "recovered"
      }
      val f2 = success(Input(2))(ResourceRequest(1, 500))
      val f3 = f1.flatMap(_ =>
        fail(Input(1))(ResourceRequest(1, 500)).handleError {
          case _: TestException => "recovered"
        }
      )
      val future = for {
        t1 <- f1
        t2 <- f2
        t3 <- f3
      } yield (t1, t2, t3)

      (future)

    }
  }

}

class FailingTasksTestSuite extends FunSuite with Matchers {

  test(
    "a failing task should propagate its exception and not interfere with other tasks"
  ) {
    FailingTasksTest.run.unsafeRunSync().get shouldBe (("recovered", "succeeded", "recovered"))
  }

}
