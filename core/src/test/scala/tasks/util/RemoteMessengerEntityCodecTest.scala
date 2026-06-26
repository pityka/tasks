/*
 * The MIT License
 *
 * Copyright (c) 2026 Istvan Bartha
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

package tasks.util

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.http4s.{MediaType, Method, Request}
import org.http4s.headers.`Content-Type`
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import tasks.queue.{Base64Data, UntypedResult}
import tasks.util.message.{Address, Message, MessageData}

class RemoteMessengerEntityCodecTest extends AnyFunSuite with Matchers {

  test(
    "decode a Message whose JSON body exceeds the default jsoniter maxCharBufSize"
  ) {
    // jsoniter's default maxCharBufSize is 128 KiB. We embed a single string
    // field >> that limit to ensure the decoder is exercised at the boundary
    // that caused the production HTTP 400 ("too long string exceeded
    // 'maxCharBufSize'").
    val largePayload = "x" * (1024 * 1024) // 1 MiB
    val original = Message(
      data = MessageData.MessageFromTask(
        result = UntypedResult(
          files = Set.empty,
          data = Base64Data(largePayload),
          mutableFiles = None
        ),
        retrievedFromCache = false
      ),
      from = Address("from"),
      to = Address("to")
    )

    val request = Request[IO](method = Method.POST)
      .withContentType(`Content-Type`(MediaType.application.json))
      .withEntity(original)(RemoteMessenger.entityEncoder)

    val decoded = request
      .as[Message](implicitly, RemoteMessenger.entityDecoder)
      .unsafeRunSync()

    decoded shouldBe original
  }
}
