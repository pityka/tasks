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
import cats.effect.unsafe.implicits.global

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}

import org.scalatest.matchers.should.Matchers

import tasks.jsonitersupport._
import org.ekrich.config.ConfigFactory
import cats.effect.IO

object CacheWithoutFilePrefixTest extends TestHelpers {

  val sideEffect = scala.collection.mutable.ArrayBuffer[String]()

  val task = Task[Input, Int]("cachewithoutfileprefix", 1) { _ => _ =>
    IO {
      sideEffect += "execution of task"
      1
    }
  }

  def run = {

    val testConfig2 = {
      val tmp = tasks.util.TempFile.createTempFile(".temp")
      tmp.delete
      ConfigFactory.parseString(
        s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=4
      tasks.createFilePrefixForTaskId = false
      """
      )
    }

    withTaskSystem(testConfig2) { implicit ts =>
      val future = for {
        t1 <- task(Input(1))(ResourceRequest(1, 500))
        t2 <- task(Input(1))(ResourceRequest(1, 500))
        t3 <- task(Input(2))(ResourceRequest(1, 500))
      } yield t1 + t2 + t3

      future

    }
  }

}

class CacheWithoutFilePrefixTestSuite extends FunSuite with Matchers {

  test(
    "caching should work even if with  tasks.createFilePrefixForTaskId = false"
  ) {
    CacheWithoutFilePrefixTest.run.unsafeRunSync().toOption.get shouldBe 3
    CacheWithoutFilePrefixTest.sideEffect.count(
      _ == "execution of task"
    ) shouldBe 2
  }

}
