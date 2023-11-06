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
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  import com.github.plokhotnyuk.jsoniter_scala.core._
  case class SporeDTO(fqcn: String, deps: Seq[SporeDTO]) {
    def toSpore[A, B]: Spore[A, B] = Spore(fqcn, deps.map(_.toSpore))
  }
  object SporeDTO {
    def apply[A, B](spore: Spore[A, B]): SporeDTO =
      SporeDTO(fqcn = spore.fqcn, deps = spore.dependencies.map(SporeDTO(_)))
  }
  implicit val codec1: JsonValueCodec[SporeDTO] = JsonCodecMaker.make(
    CodecMakerConfig.withAllowRecursiveTypes(true)
  )

  implicit def codec[A, B]: JsonValueCodec[Spore[A, B]] =
    new JsonValueCodec[Spore[A, B]] {
      val nullValue: Spore[A, B] = null.asInstanceOf[Spore[A, B]]

      def encodeValue(x: Spore[A, B], out: JsonWriter): _root_.scala.Unit = {
        val dto = SporeDTO(x)
        codec1.encodeValue(dto, out)
      }

      def decodeValue(in: JsonReader, default: Spore[A, B]): Spore[A, B] = {
        val dto = codec1.decodeValue(in, codec1.nullValue)
        dto.toSpore[A, B]
      }
    }

}
