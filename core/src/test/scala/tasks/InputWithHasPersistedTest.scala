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
import cats.effect.IO
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
object InputWithHasPersistentTest extends TestHelpers {

  val sideEffect = scala.collection.mutable.ArrayBuffer[String]()

  case class InputWithHasPersistent(
      ephemeral: Option[Int],
      persisted: Option[Int]
  ) extends HasPersistent[InputWithHasPersistent] {
    def persistent = InputWithHasPersistent(None, persisted)
  }
  object InputWithHasPersistent {
    implicit val codec: JsonValueCodec[InputWithHasPersistent] =
      JsonCodecMaker.make

  }

  val task = Task[InputWithHasPersistent, Int]("haspersistenttest", 1) {
    _ => _ =>
      sideEffect += "execution of task"
      IO(1)
  }

  def run = {
    withTaskSystem(testConfig) { implicit ts =>
      val future = for {
        t1 <- task(InputWithHasPersistent(Some(1), Some(1)))(
          ResourceRequest(1, 500)
        )
        t2 <- task(InputWithHasPersistent(Some(2), Some(1)))(
          ResourceRequest(1, 500)
        )
        t3 <- task(InputWithHasPersistent(Some(2), Some(2)))(
          ResourceRequest(1, 500)
        )
      } yield t1 + t2 + t3

      (future)

    }
  }

}

class InputWithHasPersistentTestSuite extends FunSuite with Matchers {

  test(
    "a failing task should propagate its exception and not interfere with other tasks"
  ) {
    InputWithHasPersistentTest.run.unsafeRunSync().toOption.get shouldBe 3
    InputWithHasPersistentTest.sideEffect.count(
      _ == "execution of task"
    ) shouldBe 2
  }

}
