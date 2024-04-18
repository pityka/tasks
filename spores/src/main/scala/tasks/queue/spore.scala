package tasks.queue

case class SporeException(message: String, cause: Throwable)
    extends Exception(message, cause)

case class Spore[A, B](fqcn: String, dependencies: Seq[Spore[Any, Any]])
    extends Revivable[A, B] {
  def as[A0, B0] = Spore[A0, B0](fqcn, dependencies)

  lazy val revived = revive

  def apply(a: A): B = {
    val r: SporeFun[A, B] = revived

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
