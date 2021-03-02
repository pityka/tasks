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
    val available = ResourceAvailable(0, 0, 0, List(1, 0, 1, 0))
    val alloc1 = ResourceAllocated(0, 0, 0, List(0, 0))
    val alloc2 = ResourceAllocated(0, 0, 0, List(0, 1))
    assert(available.canFulfillRequest(alloc1))
    assert(available.canFulfillRequest(alloc2))
    assert(
      available.substract(alloc2) == ResourceAvailable(0, 0, 0, List(1, 0))
    )
    assert(
      available.substract(alloc1) == ResourceAvailable(0, 0, 0, List(1, 1))
    )
  }

}
