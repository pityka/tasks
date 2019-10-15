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
import scala.concurrent.ExecutionContext.Implicits.global

object NodeLocalCacheTest extends TestHelpers {

  val sideEffect = scala.collection.mutable.ArrayBuffer[String]()

  def cachedFunction(tag: String) = synchronized {
    sideEffect += "execution of nodelocalcache factory " + tag
    Thread.sleep(1000)

    1
  }

  val testTask = AsyncTask[Input, Int]("nodelocalcachetest", 1) {
    input => implicit computationEnvironment =>
      synchronized {
        sideEffect += "execution of task"
      }
      tasks.queue.NodeLocalCache
        .getItem("key" + input.i % 2) {
          cachedFunction((input.i % 2).toString)
        }
  }

  def run = {
    withTaskSystem(testConfig) { implicit ts =>
      val f1 = testTask(Input(1))(ResourceRequest(1, 500))
      val f2 = testTask(Input(2))(ResourceRequest(1, 500))
      val f3 = testTask(Input(3))(ResourceRequest(1, 500))
      val future = for {
        t1 <- f1
        t2 <- f2
        t3 <- f3
        _ = tasks.queue.NodeLocalCache.drop("key0")
        t4 <- testTask(Input(4))(ResourceRequest(1, 500))
      } yield t1 + t2 + t3 + t4

      await(future)

    }
  }

}

class NodeLocalCacheTestSuite extends FunSuite with Matchers {

  test("node local cache should not execute the same key twice, unless dropped") {
    NodeLocalCacheTest.run.get should equal(4)
    println(NodeLocalCacheTest.sideEffect)
    NodeLocalCacheTest.sideEffect.count(_ == "execution of task") shouldBe 4
    NodeLocalCacheTest.sideEffect.count(
      _ == "execution of nodelocalcache factory 1"
    ) shouldBe 1
    NodeLocalCacheTest.sideEffect.count(
      _ == "execution of nodelocalcache factory 0"
    ) shouldBe 2
  }

}
