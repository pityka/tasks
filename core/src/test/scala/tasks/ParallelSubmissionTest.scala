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
import scribe.modify.LogModifier

object ParallelSubmissionTest {

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

  // Per-GPU "in use" flag. Tasks acquire/release it via bracket so the
  // flag is reset on natural completion AND on cancellation (preemption).
  // The acquire-time assertion catches double-allocation of any GPU id.
  val externalCounter = new java.util.concurrent.ConcurrentHashMap[Int, Boolean]
  externalCounter.put(0, false)
  externalCounter.put(1, false)
  externalCounter.put(2, false)
  externalCounter.put(3, false)

  val fibtask: TaskDefinition[FibInput, FibOut] =
    Task[FibInput, FibOut]("fib", 1) {

      case FibInput(Some(n), Some(tag)) => { implicit ce =>
        assert(ce.resourceAllocated.gpu.size == 1)
        val gpu = ce.resourceAllocated.gpu.head

        val acquire = IO {
          if (externalCounter.get(gpu))
            throw new AssertionError(
              s"GPU $gpu allocated concurrently to two tasks"
            )
          externalCounter.put(gpu, true)
        }

        def release(): IO[Unit] =
          IO(externalCounter.put(gpu, false)).void

        acquire.bracket { _ =>
          n match {
            case 0 => IO.pure(FibOut(0))
            case 1 => IO.pure(FibOut(1))
            case nn =>
              val f1 = fibtask(FibInput(Some(nn - 1), Some(false :: tag)))(
                ResourceRequest(cpu = (1, 1), memory = 1, gpu = 1, scratch = 1)
              )
              val f2 = fibtask(FibInput(Some(nn - 2), Some(true :: tag)))(
                ResourceRequest(cpu = (1, 1), memory = 1, gpu = 1, scratch = 1)
              )
              IO.both(f1, f2).map { case (a, b) => FibOut(a.n + b.n) }
          }
        }(_ => release())
      }

      case _ => ???

    }

}

class RecursiveParallelSubmissionTestSuite
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
tasks.disableRemoting = true
hosts.numCPU=400
hosts.scratch = 4000
hosts.gpus = [0,1,2,3]
tasks.askInterval = 20 ms
tasks.failuredetector.heartbeat-interval = 200 ms
      tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      """
    )
  }

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    // .withHandler(minimumLevel = Some(scribe.Level.Debug))
    // .withModifier(scribe.filter.FilterBuilder(select = List(scribe.filter.className.startsWith("tasks.queue.Launcher"))).excludeUnselected)
    .replace()

  val pair = defaultTaskSystem(Some(testConfig)).allocated.unsafeRunSync()
  implicit val system: TaskSystemComponents = pair._1._1
  import ParallelSubmissionTest._

  test("parallel submission of recursive fibonacci with GPU exclusivity") {
    IO.parSequenceN(500)((1 to 3000).toList.map { n0 =>
      {
        val n = math.min(n0, 8)
        val r = (fibtask(FibInput(n))(
          ResourceRequest(cpu = (1, 1), memory = 1, gpu = 1, scratch = 1)
        )).map(_.n)
        r.map { r =>
          assertResult(r)(serial(n))
        }
      }
    }).unsafeRunSync()
  }

  override def afterAll() = {
    pair._2.unsafeRunSync()
  }
}
