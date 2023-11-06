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
import org.scalatest.matchers.should._
import org.scalatest._
import com.typesafe.config.ConfigFactory

import tasks.jsonitersupport._

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import tasks.releaseResourcesEarly

object RercursiveTasksTest {

  def serial(n: Int): Int = n match {
    case 0 => 0
    case 1 => 1
    case _ => serial(n - 1) + serial(n - 2)
  }

  case class FibInput(n: Option[Int], tag: Option[List[Boolean]])

  object FibInput {
    implicit val codec: JsonValueCodec[FibInput] = JsonCodecMaker.make

    def apply(n: Int): FibInput = FibInput(Some(n), tag = Some(Nil))
  }

  case class FibOut(n: Int)
  object FibOut {
    implicit val codec: JsonValueCodec[FibOut] = JsonCodecMaker.make

  }

  val fibtask: TaskDefinition[FibInput, FibOut] =
    Task[FibInput, FibOut]("fib", 1) {

      case FibInput(Some(n), Some(tag)) => { implicit ce =>
        n match {
          case 0 => IO.pure(FibOut(0))
          case 1 => IO.pure(FibOut(1))
          case n => {
            val f1 = fibtask(FibInput(Some(n - 1), Some(false :: tag)))(
              ResourceRequest(1, 1)
            )
            val f2 = fibtask(FibInput(Some(n - 2), Some(true :: tag)))(
              ResourceRequest(1, 1)
            )
            for {
              _  <- releaseResourcesEarly
              r1 <- f1
              r2 = f2.unsafeRunSync()
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
    with BeforeAndAfterAll
    with TestHelpers {

  override val testConfig = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""
      akka.loglevel = "OFF"
tasks.cacheEnabled = false
tasks.disableRemoting = true
hosts.numCPU=4
      tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      """
    )
  }

  val pair = defaultTaskSystem(Some(testConfig))._1.allocated.unsafeRunSync()
  implicit val system : TaskSystemComponents = pair._1
  import RercursiveTasksTest._

  test("recursive fibonacci") {
    val n = 16
    val r = await(fibtask(FibInput(n))(ResourceRequest(1, 1))).n
    assertResult(r)(serial(n))
  }

  override def afterAll() = {
    Thread.sleep(1500)
    pair._2.unsafeRunSync()

  }
}
