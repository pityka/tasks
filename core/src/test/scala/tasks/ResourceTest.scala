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

import tasks.shared._
import org.scalatest.funsuite.{AnyFunSuite => FunSuite}

import org.scalatest.matchers.should.Matchers

class ResourceTest extends FunSuite with Matchers {

  test("resource with gpu") {
    val available = ResourceAvailable(0, 0, 0, List(0, 1, 2, 3), None)
    val alloc1 = ResourceAllocated(0, 0, 0, List(0, 1), None)
    val alloc2 = ResourceAllocated(0, 0, 0, List(2, 3), None)
    assert(available.canFulfillRequest(alloc1))
    assert(available.canFulfillRequest(alloc2))
    assert(
      available
        .substract(alloc1) == ResourceAvailable(0, 0, 0, List(2, 3), None)
    )
    assert(
      available
        .substract(alloc2) == ResourceAvailable(0, 0, 0, List(0, 1), None)
    )
  }

  test("substract then addBack returns to original state with gpus") {
    val available = ResourceAvailable(0, 0, 0, List(0, 1, 2, 3), None)
    val alloc1 = ResourceAllocated(0, 0, 0, List(0, 1), None)
    val alloc2 = ResourceAllocated(0, 0, 0, List(2, 3), None)
    val afterFirst = available.substract(alloc1)
    val afterSecond = afterFirst.substract(alloc2)
    afterSecond.gpu shouldBe Nil
    val afterFirstReturn = afterSecond.addBack(alloc2)
    val afterSecondReturn = afterFirstReturn.addBack(alloc1)
    afterSecondReturn shouldBe available
  }

  test("ResourceAvailable rejects unsorted or duplicated gpu ids") {
    intercept[IllegalArgumentException](
      ResourceAvailable(0, 0, 0, List(1, 0), None)
    )
    intercept[IllegalArgumentException](
      ResourceAvailable(0, 0, 0, List(0, 0, 1), None)
    )
  }

  test("ResourceAllocated rejects unsorted or duplicated gpu ids") {
    intercept[IllegalArgumentException](
      ResourceAllocated(0, 0, 0, List(1, 0), None)
    )
    intercept[IllegalArgumentException](
      ResourceAllocated(0, 0, 0, List(0, 0), None)
    )
  }

}
