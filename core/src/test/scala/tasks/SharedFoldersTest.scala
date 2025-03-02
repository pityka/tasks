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

package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import cats.effect.unsafe.implicits.global

import org.scalatest.matchers.should.Matchers

import tasks.util._
import tasks.jsonitersupport._
import tasks.fileservice.ManagedFilePath

import com.typesafe.config.ConfigFactory
import java.io.File

object SharedFoldersTest extends TestHelpers {

  val increment = Task[Input, SharedFile]("sharedFolders", 1) {
    _ => implicit computationEnvironment =>
      SharedFile
        .fromFolder(1) { directory =>
          val intermediateFolder = new File(directory, "intermediate")
          intermediateFolder.mkdir
          val file = new File(intermediateFolder, "fileName")
          val os = new java.io.FileOutputStream(file)
          os.write(Array[Byte](51, 52, 53))
          os.close
          List(file)
        }
        .map(_.head)

  }

  val tmp = TempFile.createTempFile(".temp")
  tmp.delete
  def run = {
    withTaskSystem(
      Some(
        ConfigFactory.parseString(
          s"tasks.fileservice.storageURI=${tmp.getAbsolutePath}\n"
        )
      )
    ) { implicit ts =>
      ((increment(Input(0))(ResourceRequest(1, 500))))

    }
  }

}

class SharedFoldersTestSuite extends FunSuite with Matchers {
  import cats.effect.unsafe.implicits.global

  test("SharedFile.fromFolder should import intermediate folders ") {
    val sf = SharedFoldersTest.run.unsafeRunSync().get
    val expectedFile = new File(
      SharedFoldersTest.tmp.getAbsolutePath + "/sharedFolders/intermediate/fileName"
    )
    sf.path.asInstanceOf[ManagedFilePath].pathElements shouldBe Vector(
      "sharedFolders",
      "intermediate",
      "fileName"
    )
    expectedFile.canRead shouldBe true
    scala.io.Source.fromFile(expectedFile).mkString shouldBe "345"
  }

}
