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
import tasks.releaseResourcesEarly
import scribe.modify.LogModifier

object ParallelSubmissionTestNonRecursive {

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

  val externalCounter = new java.util.concurrent.ConcurrentHashMap[Int, Boolean]
  externalCounter.put(0, false)
  externalCounter.put(1, false)
  externalCounter.put(2, false)
  externalCounter.put(3, false)

  val fibtask: TaskDefinition[FibInput, FibOut] =
    Task[FibInput, FibOut]("fib", 1) {

      case FibInput(Some(n), Some(tag)) => { implicit ce =>
        IO {
          assert(ce.resourceAllocated.gpu.size == 1)
          val gpu = ce.resourceAllocated.gpu.head
          assert(externalCounter.get(gpu) == false)
          externalCounter.put(gpu, true)
          val r = serial(n)
          Thread.sleep(scala.util.Random.nextLong(2000))
          externalCounter.put(gpu, false)
          FibOut(r)
        }
      }
      case _ => ???
    }

}

class NonRecursiveParallelSubmissionTestSuite
    extends FunSuite
    with Matchers
    with BeforeAndAfterAll
    with TestHelpers {

  override val testConfig = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""
      
tasks.cache.enabled = false
tasks.disableRemoting = true
hosts.numCPU=200
hosts.scratch = 4000
hosts.gpus = [0,1,2,3]
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
  import ParallelSubmissionTestNonRecursive._

  test("parallel submission of non-recursive task with 'gpu' counting ") {
    IO.parSequenceN(500)((1 to 10000).toList.map { n1 =>
      IO {
        val n = math.min(30, n1)
        val r = (fibtask(FibInput(n))(
          ResourceRequest(cpu = (1, 1), memory = 1, gpu = 1, scratch = 1)
        )).map(_.n)
        r.map { r =>
          assertResult(r)(serial(n))
        }
      }.flatten
    }).unsafeRunSync()
  }

  override def afterAll() = {
    Thread.sleep(1500)
    pair._2.unsafeRunSync()

  }
}
