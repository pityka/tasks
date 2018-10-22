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
import akka.stream.scaladsl.Source
import akka.util.ByteString
import io.circe.generic.semiauto._

import scala.concurrent.Future

object ResultWithSharedFilesTest extends TestHelpers {

  val sideEffect = scala.collection.mutable.ArrayBuffer[String]()

  case class Intermediate(sf: SharedFile) extends ResultWithSharedFiles(sf)

  object Intermediate {
    implicit val enc = deriveEncoder[Intermediate]
    implicit val dec = deriveDecoder[Intermediate]
  }

  case class Output(sf1: SharedFile, sf2: SharedFile, recursive: Intermediate)
      extends ResultWithSharedFiles((recursive.files ++ List(sf1, sf2)): _*)

  object Output {
    implicit val enc = deriveEncoder[Output]
    implicit val dec = deriveDecoder[Output]
  }

  val testTask = AsyncTask[Input, Output]("resultwithsharedfilestest", 1) {
    input => implicit computationEnvironment =>
      sideEffect += "execution of task"
      val source = Source.single(ByteString("abcd"))
      val f1 = SharedFile(source, "f1")
      val f2 = SharedFile(source, "f2")
      val f3 = SharedFile(source, "f3")

      for {
        sf1 <- f1
        sf2 <- f2
        sf3 <- f3
      } yield Output(sf1, sf2, Intermediate(sf3))
  }

  def run = {
    import scala.concurrent.ExecutionContext.Implicits.global

    withTaskSystem(testConfig) { implicit ts =>
      val f1 = testTask(Input(1))(ResourceRequest(1, 500))
      val f2 = testTask(Input(1))(ResourceRequest(1, 500))
      val future = for {
        t1 <- f1
        t1Files <- Future.sequence(t1.files.map(_.file))
        t2 <- f2
        t2Files <- Future.sequence(t2.files.map(_.file))
      } yield (t1Files, t2Files)

      await(future)

    }
  }

}

class ResultWithSharedFilesTestSuite extends FunSuite with Matchers {

  test("task output <: ResultWithSharedFiles should be cached ") {
    val (t1Files, t2Files) = ResultWithSharedFilesTest.run.get
    (t1Files zip t2Files) foreach {
      case (f1, f2) =>
        f1 shouldBe f2
        f1.length shouldBe f2.length
        f1.length > 0 shouldBe true
    }
    ResultWithSharedFilesTest.sideEffect.count(_ == "execution of task") shouldBe 1

  }

}
