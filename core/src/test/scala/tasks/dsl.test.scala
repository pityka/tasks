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

import tasks._

import org.scalatest.FunSpec
import org.scalatest.Matchers
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

import tasks.queue._
import tasks.caching._
import tasks.fileservice._
import tasks.util._

import com.typesafe.config.ConfigFactory

object Tests {

  def await[T](f: Future[T]) = Await.result(f, atMost = Duration.Inf)

  case class IntWrapper(i: Int)
  // implicit val updateCounter: UpdatePrerequisitive[STP1[Int], IntWrapper] =
  //   UpdatePrerequisitive {
  //     case (old, i: IntWrapper) => old.copy(a1 = Some(i.i))
  //   }

  val increment = Task[Int, IntWrapper]("increment", 1) {
    case c =>
      implicit computationEnvironment =>
        log.info("increment {}", c)
        IntWrapper(c + 1)
  }
  val tmp = TempFile.createTempFile(".2leveldb")
  tmp.delete
  val Some(r1) = withTaskSystem(
      Some(ConfigFactory.parseString(
              s"tasks.cache.file=${tmp.getAbsolutePath}"
          ))) { implicit ts =>
    def t1(i: Int) = increment(i)(CPUMemoryRequest(1, 500))

    (await(increment(0)(CPUMemoryRequest(1, 500))).i)

  }

}

class TaskDSLTestSuite extends FunSuite with Matchers {

  test("chains should work") {
    Tests.r1 should equal(1)
  }

}
