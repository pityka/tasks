/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Modified work, Copyright (c) 2016 Istvan Bartha

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
import io.circe.generic.semiauto._

object InputWithHasPersistentTest extends TestHelpers {

  val sideEffect = scala.collection.mutable.ArrayBuffer[String]()

  case class InputWithHasPersistent(ephemeral: Option[Int],
                                    persisted: Option[Int])
      extends HasPersistent[InputWithHasPersistent] {
    def persistent = InputWithHasPersistent(None, persisted)
  }
  object InputWithHasPersistent {
    implicit val enc = deriveEncoder[InputWithHasPersistent]
    implicit val dec = deriveDecoder[InputWithHasPersistent]
  }

  val task = AsyncTask[InputWithHasPersistent, Int]("haspersistenttest", 1) {
    input => implicit computationEnvironment =>
      sideEffect += "execution of task"
      Future(1)
  }

  def run = {
    withTaskSystem(testConfig) { implicit ts =>
      import scala.concurrent.ExecutionContext.Implicits.global

      val future = for {
        t1 <- task(InputWithHasPersistent(Some(1), Some(1)))(
          CPUMemoryRequest(1, 500))
        t2 <- task(InputWithHasPersistent(Some(2), Some(1)))(
          CPUMemoryRequest(1, 500))
        t3 <- task(InputWithHasPersistent(Some(2), Some(2)))(
          CPUMemoryRequest(1, 500))
      } yield t1 + t2 + t3

      await(future)

    }
  }

}

class InputWithHasPersistentTestSuite extends FunSuite with Matchers {

  test(
    "a failing task should propagate its exception and not interfere with other tasks") {
    InputWithHasPersistentTest.run.get shouldBe 3
    InputWithHasPersistentTest.sideEffect.count(_ == "execution of task") shouldBe 2
  }

}
