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
import scala.concurrent.Future
import io.circe.generic.semiauto._
import com.typesafe.config.ConfigFactory

object SchemaEvolutionViaOptionsTest extends TestHelpers {

  val sideEffect = scala.collection.mutable.ArrayBuffer[String]()

  val testConfig2 = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
    hosts.numCPU=4
    tasks.createFilePrefixForTaskId = false
    akka.loglevel=OFF
    """
    )
  }

  case class Input1(data1: Int)
  object Input1 {
    implicit val enc = deriveEncoder[Input1]
    implicit val dec = deriveDecoder[Input1]
  }

  case class Input2(data1: Int, data2: Option[Int])
  object Input2 {
    implicit val enc = deriveEncoder[Input2]
    implicit val dec = deriveDecoder[Input2]
  }

  val task1 = AsyncTask[Input1, Int]("schemaevolutionviaoptions", 1) {
    _ => implicit computationEnvironment =>
      sideEffect += "execution of task"
      Future(1)
  }

  val task2 = AsyncTask[Input2, Int]("schemaevolutionviaoptions", 1) {
    _ => implicit computationEnvironment =>
      sideEffect += "execution of task"
      Future(1)
  }

  def run = {
    val run1 = withTaskSystem(testConfig2) { implicit ts =>
      import scala.concurrent.ExecutionContext.Implicits.global

      val future = for {
        t1 <- task1(Input1(1))(ResourceRequest(1, 500))
      } yield t1

      await(future)

    }

    val run2 = withTaskSystem(testConfig2) { implicit ts =>
      import scala.concurrent.ExecutionContext.Implicits.global

      val future = for {
        t1 <- task2(Input2(1, None))(ResourceRequest(1, 500))
      } yield t1

      await(future)

    }

    val run3 = withTaskSystem(testConfig2) { implicit ts =>
      import scala.concurrent.ExecutionContext.Implicits.global

      val future = for {
        t1 <- task2(Input2(1, Some(2)))(ResourceRequest(1, 500))
      } yield t1

      await(future)

    }

    run1.get + run2.get + run3.get
  }

}

class SchemaEvolutionViaOptionsTestSuite extends FunSuite with Matchers {

  test("schema evolution via options should work") {
    SchemaEvolutionViaOptionsTest.run shouldBe 3
    SchemaEvolutionViaOptionsTest.sideEffect.count(_ == "execution of task") shouldBe 2
  }

}
