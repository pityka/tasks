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

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}

import org.scalatest.matchers.should.Matchers
import scala.concurrent._

import tasks.util._
import tasks.circesupport._

import com.typesafe.config.ConfigFactory

object ExecOnceTest extends TestHelpers with Matchers {

  val sideEffect = scala.collection.mutable.ArrayBuffer[String]()

  val increment = AsyncTask[Input, Int]("execonce", 1) { case Input(c) =>
    implicit computationEnvironment =>
      sideEffect += "executed"
      Future(c + 1)
  }

  def run = {
    val tmp = TempFile.createTempFile(".temp")
    tmp.delete
    withTaskSystem(
      Some(
        ConfigFactory.parseString(
          s"tasks.fileservice.storageURI=${tmp.getAbsolutePath}\nakka.loglevel=OFF"
        )
      )
    ) { implicit ts =>
      import scala.concurrent.ExecutionContext.Implicits.global
      await(Future.traverse(1 to 10000) { input =>
        increment(Input(0))(
          ResourceRequest(1, 500),
          labels = tasks.shared.Labels(List(input.toString -> input.toString))
        )
      })

    }
  }

}

class ExecOnceTestSute extends FunSuite with Matchers {

  test("same task sent multiple times should execute exactly once") {
    ExecOnceTest.run.get shouldBe (1 to 10000).map(_ => 1)
    ExecOnceTest.sideEffect.size shouldBe 1
  }

}
