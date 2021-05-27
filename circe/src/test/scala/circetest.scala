/*
 * The MIT License
 *
 * Copyright (c) 2019 Istvan Bartha
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
import org.scalatest.matchers.should._

import tasks.circesupport._
import tasks.fileservice.SharedFileHelper
import tasks.queue.{Serializer, Deserializer}
import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._

class CirceSuite extends FunSuite with Matchers {

  test("circe adt") {

    sealed trait P
    case class Leaf1(a: String) extends P
    case class Leaf2(a: Int) extends P

    implicit val enc: Encoder[Leaf1] = deriveEncoder[Leaf1]
    implicit val dec: Decoder[Leaf1] = deriveDecoder[Leaf1]
    implicit val enc1: Encoder[P] = deriveEncoder[P]
    implicit val dec1: Decoder[P] = deriveDecoder[P]

    parser
      .decode[P]((Leaf1("a"): P).asJson.noSpaces)
      .toOption
      .get shouldBe Leaf1(
      "a"
    )
    parser
      .decode[Leaf1](Leaf1("a").asJson.noSpaces)
      .toOption
      .get shouldBe Leaf1(
      "a"
    )
  }

  test("sharedfile") {
    val sf = SharedFileHelper.createForTesting("proba")
    val sf2 = implicitly[Deserializer[SharedFile]]
      .apply(implicitly[Serializer[SharedFile]].apply(sf))

    Right(sf) shouldBe sf2
  }

}
