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

import org.scalatest.matchers.should.Matchers

import tasks.util.TempFile

object DeleteSharedFileTest extends TestHelpers {

  def run = {

    withTaskSystem(testConfig) { implicit ts =>
      val file = TempFile.createTempFile("")
      val os = new java.io.FileOutputStream(file)
      os.write(Array[Byte](51, 52, 53))
      os.close

      val future = for {
        sf <- SharedFile(file, "boo", deleteFile = true)
        local <- sf.file.allocated
          .map(_._1)
        _ <- sf.delete
      } yield local

      await(future)

    }
  }

}

class DeleteSharedFileTestSuite extends FunSuite with Matchers {

  test("SharedFile.delete should not delete a file unless config flag is set") {
    DeleteSharedFileTest.run.get.canRead shouldBe true

  }

}
