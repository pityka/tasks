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
import cats.effect.unsafe.implicits.global

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}

import org.scalatest.matchers.should.Matchers

import tasks.util._
import tasks.jsonitersupport._

import com.typesafe.config.ConfigFactory
import cats.effect.IO

object DSLTest extends TestHelpers with Matchers {

  val increment = Task[Input, Int]("dsltest", 1) { case Input(c) =>
    implicit computationEnvironment =>
      import tasks.fileservice.HistoryContextImpl
      computationEnvironment.components.historyContext
        .asInstanceOf[HistoryContextImpl]
        .task shouldBe fileservice.History
        .TaskVersion("dsltest", 1)
      computationEnvironment.components.historyContext
        .asInstanceOf[HistoryContextImpl]
        .codeVersion shouldBe "undefined"
      IO(c + 1)
  }

  def run = {
    val tmp = TempFile.createTempFile(".temp")
    tmp.delete
    withTaskSystem(
      Some(
        ConfigFactory.parseString(
          s"tasks.fileservice.storageURI=${tmp.getAbsolutePath}\n"
        )
      )
    ) { implicit ts =>
      ((increment(Input(0))(ResourceRequest(1, 500)))).flatMap { i =>
        IO(scribe.info(s"Incremented $i")).map(_ => i)

      }

    }
  }

}

class TaskDSLTestSuite extends FunSuite with Matchers {

  test("chains should work") {
    DSLTest.run.unsafeRunSync().get should equal(1)
  }

}
