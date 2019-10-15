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

import org.scalatest._

import tasks.jsonitersupport._
import tasks.fileservice.SharedFileHelper
import tasks.queue.{Serializer, Deserializer}

class JsonIterSuite extends FunSuite with Matchers {

  test("jsoniter adt") {
    import com.github.plokhotnyuk.jsoniter_scala.core._
    import com.github.plokhotnyuk.jsoniter_scala.macros._
    sealed trait P
    case class Leaf1(a: String) extends P
    case class Leaf2(a: Int) extends P

    object Codec1 {
      implicit val leaf1Codec: JsonValueCodec[Leaf1] =
        JsonCodecMaker.make[Leaf1](CodecMakerConfig())
    }

    object Codec2 {

      implicit val pCodec: JsonValueCodec[P] =
        JsonCodecMaker.make[P](CodecMakerConfig())

    }

    import Codec1._
    import Codec2._

    readFromString[P](writeToString[P]((Leaf1("a")))) shouldBe Leaf1("a")
    readFromString[Leaf1](writeToString[Leaf1]((Leaf1("a")))) shouldBe Leaf1(
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
