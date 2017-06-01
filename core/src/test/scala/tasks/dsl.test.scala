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

import org.scalatest._

import tasks._

import org.scalatest.FunSpec
import org.scalatest.Matchers
import scala.concurrent._
import scala.concurrent.duration._

import tasks.queue._
import tasks.caching._
import tasks.fileservice._
import tasks.util._
import tasks.circesupport._

import com.typesafe.config.ConfigFactory
import io.circe._
import io.circe.generic.semiauto._

object Tests {

  def await[T](f: Future[T]) = Await.result(f, atMost = Duration.Inf)

  case class IntWrapper(i: Int)
  object IntWrapper {
    implicit val enc : Encoder[IntWrapper] = deriveEncoder[IntWrapper]
    implicit val dec : Decoder[IntWrapper] = deriveDecoder[IntWrapper]
  }

  val increment = AsyncTask[IntWrapper, Int]("incrementtask", 1) {
    case IntWrapper(c) =>
    println(c)
      implicit computationEnvironment =>
        println("increment")
        Future(c + 1)
  }

  def run ={
    val tmp = TempFile.createTempFile(".temp")
    tmp.delete
     withTaskSystem(
      Some(ConfigFactory.parseString(
              s"tasks.fileservice.storageURI=${tmp.getAbsolutePath}"
          ))) { implicit ts =>

    (await(increment(IntWrapper(0))(CPUMemoryRequest(1, 500))))

  }
}

}

class TaskDSLTestSuite extends FunSuite with Matchers {

  test("chains should work") {
    Tests.run.get should equal(1)
  }

}
