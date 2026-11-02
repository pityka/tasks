package tasks.queue

private[tasks] case class SporeException(message: String, cause: Throwable)
    extends Exception(message, cause)

sealed trait Spore[A, B] extends Revivable[A, B] {
  def fqcn: String
  def dependencies: Seq[Spore[Any, Any]]

  final def as[A0, B0]: Spore[A0, B0] = Spore[A0, B0](fqcn, dependencies)

  final lazy val revived = revive

  final def apply(a: A): B = {
    val r: SporeFun[A, B] = revived
    r.call(a)
  }
}

object Spore {
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  import com.github.plokhotnyuk.jsoniter_scala.core._

  private[tasks] final case class SporeImpl[A, B](
      fqcn: String,
      dependencies: Seq[Spore[Any, Any]]
  ) extends Spore[A, B]

  def apply[A, B](fqcn: String, dependencies: Seq[Spore[Any, Any]]): Spore[A, B] =
    SporeImpl[A, B](fqcn, dependencies)

  def unapply[A, B](s: Spore[A, B]): Some[(String, Seq[Spore[Any, Any]])] =
    Some((s.fqcn, s.dependencies))

  private[tasks] case class SporeDTO(fqcn: String, deps: Seq[SporeDTO]) {
    def toSpore[A, B]: Spore[A, B] = Spore(fqcn, deps.map(_.toSpore))
  }
  private[tasks] object SporeDTO {
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
