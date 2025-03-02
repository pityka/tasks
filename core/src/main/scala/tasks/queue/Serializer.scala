package tasks.queue

import tasks.spore
import cats.effect.IO

trait Serializer[A] {
  def apply(a: A): Array[Byte]
  def hash(a: A): IO[String] = fs2.Stream
    .chunk(fs2.Chunk.array(apply(a)))
    .through(
      fs2.hashing.Hashing[IO].hash(fs2.hashing.HashAlgorithm.SHA256)
    )
    .compile
    .lastOrError
    .map(_.toString())
}

trait Deserializer[A] {
  def apply(in: Array[Byte]): Either[String, A]
}

object Serializer {
  val nothing: Serializer[Nothing] = new Serializer[Nothing] {
    def apply(a: Nothing): Array[Byte] = Array.empty
  }
}
object Deserializer {
  val nothing = new Deserializer[Nothing] {
    def apply(in: Array[Byte]) = Left("deserializing into nothing?")
  }
}

case class SerDe[AA](
    ser: Spore[Unit, Serializer[AA]],
    deser: Spore[Unit, Deserializer[AA]]
)

object SerDe {
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  import com.github.plokhotnyuk.jsoniter_scala.core._

  implicit def codec[A]: JsonValueCodec[SerDe[A]] = JsonCodecMaker.make

  implicit def makeFromComponents[A](implicit
      r: tasks.SDeserializer[A],
      w: tasks.SSerializer[A]
  ): SerDe[A] =
    SerDe(w, r)

  val ns: Spore[Unit, Serializer[Nothing]] =
    spore((_: Unit) => Serializer.nothing)

}
