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
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import com.typesafe.config.ConfigFactory
import cats.effect.IO
import cats.Applicative

object SchemaEvolutionViaOptionsTest extends TestHelpers {

  val sideEffect = scala.collection.mutable.ArrayBuffer[String]()

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

  case class Input1(data1: Int)
  object Input1 {
    implicit val codec: JsonValueCodec[Input1] = JsonCodecMaker.make

  }

  case class Input2(data1: Int, data2: Option[Int])
  object Input2 {
    implicit val codec: JsonValueCodec[Input2] = JsonCodecMaker.make
  }

  val task1 = Task[Input1, Int]("schemaevolutionviaoptions", 1) { _ => _ =>
    sideEffect += "execution of task"
    IO(1)
  }

  val task2 = Task[Input2, Int]("schemaevolutionviaoptions", 1) { _ => _ =>
    sideEffect += "execution of task"
    IO(1)
  }

  def run = {
    val run1 = withTaskSystem(testConfig2) { implicit ts =>
      val future = for {
        t1 <- task1(Input1(1))(ResourceRequest(1, 500))
      } yield t1

      (future)

    }

    val run2 = withTaskSystem(testConfig2) { implicit ts =>
      val future = for {
        t1 <- task2(Input2(1, None))(ResourceRequest(1, 500))
      } yield t1

      (future)

    }

    val run3 = withTaskSystem(testConfig2) { implicit ts =>
      val future = for {
        t1 <- task2(Input2(1, Some(2)))(ResourceRequest(1, 500))
      } yield t1

      (future)

    }

    for {
      run1 <- run1 
      run2 <- run2 
      run3 <- run3
    } yield (run1.get + run2.get + run3.get)
    
  }

}

class SchemaEvolutionViaOptionsTestSuite extends FunSuite with Matchers {

  test("schema evolution via options should work") {
    SchemaEvolutionViaOptionsTest.run.unsafeRunSync() shouldBe 3
    SchemaEvolutionViaOptionsTest.sideEffect.count(
      _ == "execution of task"
    ) shouldBe 2
  }

}
