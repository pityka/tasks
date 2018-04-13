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

package tasks.collection

import org.scalatest._

import tasks._
import tasks.circesupport._

import scala.concurrent._
import scala.concurrent.duration._
import io.circe.generic.auto._

object Tests {

  val twice =
    EColl.map("twice", 1)((_: Int) * 3)

  val odd = EColl.filter("odd", 1)((_: Int) % 1 == 0)

  val sort = EColl.sortBy("sortByToString", 1)(4L, 10, (_: Int).toString)

  val group = EColl.groupBy("groupByToConstant", 1)(4L, 10, (_: Int) => "a")

  val join =
    EColl.outerJoinBy("outerjoinByToString", 1)(4L, 10, (_: Int).toString)

  val count =
    EColl.foldLeft("count", 1)(0, (x: Int, y: Seq[Option[Int]]) => x + 1)

  val sum = EColl.reduce("sum", 1)((x: Int, y: Int) => x + y)

  def run(folder: String) = {

    withTaskSystem(
      s"tasks.fileservice.storageURI=$folder"
    ) { implicit ts =>
      import scala.concurrent.ExecutionContext.Implicits.global

      val mappedEColl = for {
        e1 <- EColl.fromSource(akka.stream.scaladsl.Source(List(3, 2, 1)),
                               name = "ecollint")
        e2 <- twice(e1)(CPUMemoryRequest(1, 1))
        e3 <- odd(e2)(CPUMemoryRequest(1, 1))
        e4 <- sort(e3)(CPUMemoryRequest(1, 1))
        e5 <- group(e4)(CPUMemoryRequest(1, 1))
        e6 <- join(List(e1, e2, e3))(CPUMemoryRequest(1, 1))
        e7 <- count(e6)(CPUMemoryRequest(1, 1))
        e8 <- sum(e4)(CPUMemoryRequest(1, 1))
      } yield e8

      Await.result(mappedEColl, atMost = 10 minutes)
    }

  }

}

class TaskCollectionTestSuite extends FunSuite with Matchers {

  test("collection") {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    println(tmp)
    Tests.run(tmp.getAbsolutePath).get should equal(18)
    Tests.run(tmp.getAbsolutePath).get should equal(18)
  }

}
