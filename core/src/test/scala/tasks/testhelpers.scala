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

import scala.concurrent._
import scala.concurrent.duration._

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

import com.typesafe.config.ConfigFactory
import cats.effect.IO
import cats.effect.unsafe.implicits.global
trait TestHelpers {

  def writeBinaryToFile(fileName: java.io.File, d: Array[Byte]) =
    java.nio.file.Files.write(fileName.toPath, d)

  /** Reads file contents into a bytearray. */
  def readBinaryFile(fileName: String): Array[Byte] = {
    java.nio.file.Files.readAllBytes(new java.io.File(fileName).toPath)
  }

  def await[T](f: Future[T]) = Await.result(f, atMost = 60 seconds)
  def await[T](f: IO[T]) = f
    .unsafeRunTimed(60 seconds)
    .getOrElse(throw new RuntimeException("timeout"))

  case class Input(i: Int)
  object Input {
    implicit val codec: JsonValueCodec[Input] = JsonCodecMaker.make

  }

  case class InputSF(files1: List[SharedFile]) extends WithSharedFiles(files1)
  object InputSF {
    implicit val codec: JsonValueCodec[InputSF] = JsonCodecMaker.make
  }

  def testConfig = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=4
      
      """
    )
  }

}
