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

import tasks.fileservice.SharedFileHelper
import tasks.fileservice.ManagedFilePath
import tasks.queue.UntypedResult
import tasks.queue.Base64Data
import tasks.caching.TaskSerializer

class TaskSerializerTestSuite
    extends FunSuite
    with Matchers
    with TaskSerializer {

  test("serialize and deserialize UntypedResult") {
    val sf: SharedFile =
      SharedFileHelper.create(1L, 1, ManagedFilePath(Vector("boo")))
    val v = UntypedResult(
      files = Set(sf),
      data = Base64Data("whatever"),
      mutableFiles = Some(Set(sf))
    )

    deserializeResult(serializeResult(v)) shouldBe v

  }

}
