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

import tasks.circesupport._
import scala.concurrent.Future

object EarlyShutdownTest extends TestHelpers {

  val sideEffect = scala.collection.mutable.ArrayBuffer[String]()

  val task = AsyncTask[Input, Int]("earlyshutdowntest", 1) {
    _ => implicit computationEnvironment =>
      sideEffect += "execution of task"
      Thread.sleep(2000)
      Future(1)
  }

  def run = {
    withTaskSystem(testConfig) { implicit ts =>
      task(Input(1))(ResourceRequest(1, 500))
      while (sideEffect.size < 1) {
        Thread.sleep(50)
      }
      None
    }
  }

}

class EarlyShutdownTestSuite extends FunSuite with Matchers {

  test("task system should shut down even if tasks are running") {
    EarlyShutdownTest.run.get shouldBe None
  }

}
