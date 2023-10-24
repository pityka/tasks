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
import java.io.File
import tasks.jsonitersupport._

object FolderFileStorageTest extends TestHelpers with Matchers {

  val task1 = Task[Input, Boolean]("sharedfileinput1", 1) {
    _ => implicit computationEnvironment =>
      for {

        sf <- SharedFile(
          fs2.Stream.chunk(fs2.Chunk.array("abcd".getBytes("UTF-8"))),
          "f1"
        )
        local <- sf.file.allocated.map(_._1)
        sf2 <- {
          val newPath = new File(local.getParentFile.getParentFile, "uncle")
          tasks.util.openFileOutputStream(newPath)(_.write("boo".getBytes))
          SharedFile(newPath, "something")

        }
        local2 <- sf2.file.allocated.map(_._1)
      } yield local2.canRead
  }

  def run = {
    withTaskSystem(testConfig) { implicit ts =>
      val future = for {
        t11 <- task1(Input(1))(ResourceRequest(1, 500))
      } yield t11

      await(future)

    }
  }

}

class FolderFileStorageTestSuite extends FunSuite with Matchers {

  test("importing a file relative to the base folder should work correctly") {
    FolderFileStorageTest.run.get shouldBe true
  }

}
