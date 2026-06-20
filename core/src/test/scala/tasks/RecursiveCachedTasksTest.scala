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
import org.ekrich.config.ConfigFactory

import tasks.jsonitersupport._

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import cats.effect.unsafe.implicits.global

object RecursiveCachedTaskTestSuite {

  def serial(n: Int): Int = n match {
    case 0 => 0
    case 1 => 1
    case _ => serial(n - 1) + serial(n - 2)
  }

  case class FibInput(n: Option[Int])

  object FibInput {
    implicit val codec: JsonValueCodec[FibInput] = JsonCodecMaker.make

    def apply(n: Int): FibInput = FibInput(Some(n))
  }

  case class FibOut(n: Int)
  object FibOut {
    implicit val codec: JsonValueCodec[FibOut] = JsonCodecMaker.make

  }

  val zeroCpuConcurrent = new java.util.concurrent.atomic.AtomicInteger(0)
  val zeroCpuPeak = new java.util.concurrent.atomic.AtomicInteger(0)

  val zeroCpuTrackedTask: TaskDefinition[FibInput, FibOut] =
    Task[FibInput, FibOut]("zero-cpu-tracked", 1) {
      case FibInput(Some(n)) => { implicit ce =>
        assert(ce.resourceAllocated.cpu == 0)
        for {
          now <- IO(zeroCpuConcurrent.incrementAndGet())
          _ <- IO(zeroCpuPeak.updateAndGet(p => math.max(p, now)))
          _ <- IO.sleep(scala.concurrent.duration.DurationInt(100).millis)
          _ <- IO(zeroCpuConcurrent.decrementAndGet())
        } yield FibOut(n)
      }
      case _ => ???
    }

  // Recursive fib expressed as a parent task. Parents have zero resource
  // allocation, so the recursion fans out without occupying CPU slots —
  // exactly the pattern parents are meant to express.
  val fibtaskParent: ParentTaskDefinition[FibInput, FibOut] =
    ParentTask[FibInput, FibOut]("fib-parent", 1) {

      case FibInput(Some(n)) => { implicit ce =>
        n match {
          case 0 => IO.pure(FibOut(0))
          case 1 => IO.pure(FibOut(1))
          case n =>
            val f1 = fibtaskParent(FibInput(Some(n - 1)))
            val f2 = fibtaskParent(FibInput(Some(n - 2)))
            IO.both(f1, f2).map { case (a, b) => FibOut(a.n + b.n) }
        }
      }

      case _ => ???

    }

}

class RecursiveCachedTaskTestSuite
    extends FunSuite
    with Matchers
    with BeforeAndAfterAll
    with TestHelpers {

  override val testConfig = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""

tasks.cache.enabled = true
tasks.createFilePrefixForTaskId = false
tasks.disableRemoting = true
hosts.numCPU=4
tasks.askInterval = 20 ms
tasks.failuredetector.heartbeat-interval = 200 ms
      tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      """
    )
  }

  val pair = defaultTaskSystem(Some(testConfig)).allocated.unsafeRunSync()
  implicit val system: TaskSystemComponents = pair._1._1
  import RecursiveCachedTaskTestSuite._

  test("recursive fibonacci via parent task") {
    val n = 16
    val r = fibtaskParent(FibInput(n)).unsafeRunSync().n
    assertResult(r)(serial(n))
  }

  test("many 0 cpu tasks run concurrently beyond numCPU") {
    val N = 32
    IO.parSequenceN(N)((1 to N).toList.map { i =>
      zeroCpuTrackedTask(FibInput(Some(i)))(ResourceRequest(0, 1))
    }).unsafeRunSync()
    assert(
      zeroCpuPeak.get > 4,
      s"expected peak concurrency > 4 (numCPU), got ${zeroCpuPeak.get}"
    )
  }

  override def afterAll() = {
    pair._2.unsafeRunSync()
  }
}
