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

sealed trait SerDe[AA] {
  def ser: Spore[Unit, Serializer[AA]]
  def deser: Spore[Unit, Deserializer[AA]]
}

object SerDe {
  private[tasks] final case class SerDeImpl[AA](
      ser: Spore[Unit, Serializer[AA]],
      deser: Spore[Unit, Deserializer[AA]]
  ) extends SerDe[AA]

  def apply[AA](
      ser: Spore[Unit, Serializer[AA]],
      deser: Spore[Unit, Deserializer[AA]]
  ): SerDe[AA] = SerDeImpl(ser, deser)

  def unapply[AA](
      s: SerDe[AA]
  ): Some[(Spore[Unit, Serializer[AA]], Spore[Unit, Deserializer[AA]])] =
    Some((s.ser, s.deser))

  implicit def makeFromComponents[A](implicit
      r: tasks.SDeserializer[A],
      w: tasks.SSerializer[A]
  ): SerDe[A] =
    SerDe(w, r)

  val ns: Spore[Unit, Serializer[Nothing]] =
    spore((_: Unit) => Serializer.nothing)

}
