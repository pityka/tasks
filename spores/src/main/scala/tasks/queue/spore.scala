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

package tasks.queue

case class SporeException(message: String, cause: Throwable)
    extends Exception(message, cause)

case class Spore[A, B](fqcn: String, dependencies: Seq[Spore[_, _]]) {
  def as[A0, B0] = Spore[A0, B0](fqcn, dependencies)

  private def revive = {
    try {
      val ctors = java.lang.Class
        .forName(fqcn)
        .getConstructors()

      val selectedCtor =
        ctors.find(_.getParameterCount == dependencies.size).get

      selectedCtor
        .newInstance(dependencies: _*)
        .asInstanceOf[tasks.queue.SporeFun[A, B]]
    } catch {
      case e: java.lang.NoSuchMethodException =>
        throw SporeException(
          s"Available ctors: ${java.lang.Class
            .forName(fqcn)
            .getConstructors()
            .toList}",
          e
        )
    }
  }

  lazy val revived = revive

  def apply(a: A) = {
    val r = revived

    r.call(a)
  }

}

object Spore {
  import io.circe.{Decoder, Encoder}
  @scala.annotation.nowarn
  implicit def sporeDecoder[A, B]: io.circe.Decoder[Spore[A, B]] =
    Decoder.decodeJsonObject.emap { jsonObject =>
      val fqcn = jsonObject("fqcn").flatMap(_.asString)
      val dependencies =
        jsonObject("deps").toSeq.flatMap(_.asArray.toSeq.flatten)
      fqcn match {
        case None => Left("parsing failure. needs fqcn")
        case Some(fqcn) =>
          val decodedDependencies =
            dependencies.map(d => sporeDecoder.decodeJson(d))
          if (decodedDependencies.forall(_.isRight))
            Right(Spore(fqcn, decodedDependencies.map(_.right.get)))
          else Left("parsing failure")
      }
    }

  implicit def sporeEncoder[A, B]: io.circe.Encoder[Spore[A, B]] =
    Encoder.encodeJsonObject.contramapObject[Spore[A, B]] { spore =>
      val fqcn = io.circe.Json.fromString(spore.fqcn)
      val dependencies =
        io.circe.Json.fromValues(spore.dependencies.map(d => sporeEncoder(d)))
      io.circe.JsonObject
        .fromIterable(List("fqcn" -> fqcn, "deps" -> dependencies))
    }
}
