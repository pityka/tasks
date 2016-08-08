/*
* The MIT License
*
* Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
* Group Fellay
* Copyright (c) 2016 Istvan Bartha
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

package mybiotools.tasks

import org.scalatest._

import mybiotools.tasks._

import org.scalatest.FunSpec
import org.scalatest.Matchers
import scala.concurrent.ExecutionContext.Implicits.global

object Tests {

  case class IntWrapper(i: Int) extends Result

  // case class Counter(count: Option[Int]) extends SimplePrerequisitive[Counter]

  implicit val updateCounter: UpdatePrerequisitive[STP1[Int], IntWrapper] = UpdatePrerequisitive {
    case (old, i: IntWrapper) => old.copy(a1 = Some(i.i))
  }

  val increment = TaskDefinition[STP1[Int], IntWrapper] {
    case STP1(Some(c)) => implicit computationEnvironment =>

      IntWrapper(c + 1)
  }

  val (r1, r2) = withTaskSystem { implicit ts =>

    def t1(i: Option[Int]) = increment(STP1[Int](i))(CPUMemoryRequest(1, 500))

    (
      increment(0)(CPUMemoryRequest(1, 500)).?!.i,
      (t1(Some(0)) ~> t1(None) ~> t1(None) ~> t1(None)).?!.i
    )

  }

}

class TaskDSLTestSuite extends FunSuite with Matchers {

  test("chains should work") {
    Tests.r1 should equal(1)
    Tests.r2 should equal(4)
  }

}
