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

import tasks.util.concurrent.await
import scala.concurrent._
import com.typesafe.config.ConfigFactory

import tasks.circesupport._

import io.circe.generic.auto._

object RercursiveTasksTest {

  def serial(n: Int): Int = n match {
    case 0 => 0
    case 1 => 1
    case _ => serial(n - 1) + serial(n - 2)
  }

  case class FibInput(n: Option[Int], tag: Option[List[Boolean]])

  object FibInput {
    def apply(n: Int): FibInput = FibInput(Some(n), tag = Some(Nil))
  }

  case class FibOut(n: Int)

  val fibtask: TaskDefinition[FibInput, FibOut] =
    AsyncTask[FibInput, FibOut]("fib", 1) {

      case FibInput(Some(n), Some(tag)) => { implicit ce =>
        n match {
          case 0 => Future.successful(FibOut(0))
          case 1 => Future.successful(FibOut(1))
          case n => {
            val f1 = fibtask(FibInput(Some(n - 1), Some(false :: tag)))(
              ResourceRequest(1, 1)
            )
            val f2 = fibtask(FibInput(Some(n - 2), Some(true :: tag)))(
              ResourceRequest(1, 1)
            )
            releaseResources
            for {
              r1 <- f1
              r2 = f2.awaitIndefinitely
            } yield FibOut(r1.n + r2.n)
          }

        }
      }

      case _ => ???

    }

}

class RecursiveTaskTestSuite
    extends FunSuite
    with Matchers
    with BeforeAndAfterAll {

  val testConfig = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""
      akka.loglevel = "INFO"
tasks.cacheEnabled = false
tasks.disableRemoting = true
hosts.numCPU=4
      tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      """
    )
  }

  implicit val system: TaskSystem = defaultTaskSystem(Some(testConfig))
  import RercursiveTasksTest._

  test("recursive fibonacci") {
    val n = 16
    val r = await(fibtask(FibInput(n))(ResourceRequest(1, 1))).n
    assertResult(r)(serial(n))
  }

  override def afterAll {
    Thread.sleep(1500)
    system.shutdown

  }
}
