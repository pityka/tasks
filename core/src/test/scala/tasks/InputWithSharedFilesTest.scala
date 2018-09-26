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
import akka.stream.scaladsl.Source
import akka.util.ByteString

object InputWithSharedFilesTest extends TestHelpers {

  val sideEffect = scala.collection.mutable.ArrayBuffer[String]()

  val task1 = AsyncTask[Input, SharedFile]("sharedfileinput1", 1) {
    input => implicit computationEnvironment =>
      sideEffect += "execution of task 1"
      SharedFile(Source.single(ByteString("abcd")), "f1")
  }

  val task2 = AsyncTask[SharedFile, Int]("sharedfileinput2", 1) {
    input => implicit computationEnvironment =>
      sideEffect += "execution of task 2"
      Future(1)
  }

  def run = {
    withTaskSystem(testConfig) { implicit ts =>
      import scala.concurrent.ExecutionContext.Implicits.global

      val future = for {
        t11 <- task1(Input(1))(CPUMemoryRequest(1, 500))
        t21 <- task2(t11)(CPUMemoryRequest(1, 500))
        t12 <- task1(Input(2))(CPUMemoryRequest(1, 500))
        t22 <- task2(t12)(CPUMemoryRequest(1, 500))
      } yield t21 + t22

      await(future)

    }
  }

}

class InputWithSharedFilesTestSuite extends FunSuite with Matchers {

  test(
    "a failing task should propagate its exception and not interfere with other tasks") {
    InputWithSharedFilesTest.run.get shouldBe 2
    InputWithSharedFilesTest.sideEffect.count(_ == "execution of task 1") shouldBe 2
    InputWithSharedFilesTest.sideEffect.count(_ == "execution of task 2") shouldBe 1
  }

}
