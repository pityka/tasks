package tasks.queue

import tasks.spore

trait Serializer[A] {
  def apply(a: A): Array[Byte]
}

trait Deserializer[A] {
  def apply(b: Array[Byte]): Either[String, A]
}

object Serializer {
  val nothing = new Serializer[Nothing] {
    def apply(a: Nothing) = Array.empty
  }
}
object Deserializer {
  val nothing = new Deserializer[Nothing] {
    def apply(a: Array[Byte]) = Left("deserializing into nothing?")
  }
}

case class SerDe[AA](
    ser: Spore[Unit, Serializer[AA]],
    deser: Spore[Unit, Deserializer[AA]]
)

object SerDe {
  import io.circe._
  import io.circe.generic.semiauto._
  implicit def encoder[A, B]: Encoder[SerDe[A]] = deriveEncoder[SerDe[A]]
  implicit def decoder[A, B]: Decoder[SerDe[A]] = deriveDecoder[SerDe[A]]

  implicit def makeFromComponents[A](
      implicit r: tasks.SDeserializer[A],
      w: tasks.SSerializer[A]
  ): SerDe[A] =
    SerDe(w, r)

  val nothing = SerDe[Nothing](
    ser = spore(() => Serializer.nothing),
    deser = spore(() => Deserializer.nothing)
  )
}

object SerdeMacro {
  import scala.reflect.macros.blackbox.Context

  def create[A: cxt.WeakTypeTag](cxt: Context) = {
    import cxt.universe._
    val a = weakTypeOf[A]
    val name1 = cxt.freshName(TermName("serde"))
    val name2 = cxt.freshName(TermName("serde"))

    val r = q"""
      val $name1 = _root_.tasks.spore{() => implicitly[_root_.tasks.queue.Deserializer[$a]]}
      val $name2 = _root_.tasks.spore{() => implicitly[_root_.tasks.queue.Serializer[$a]]}
      _root_.tasks.queue.SerDe($name2,$name1)
    """
    r
  }

}
