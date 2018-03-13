package tasks

import tasks.queue._

package object upicklesupport {
  import upickle.default.{Reader, Writer}
  implicit def ser[A](implicit enc: Writer[A]): Serializer[A] =
    new Serializer[A] {
      def apply(a: A) = upickle.default.write(a).getBytes("UTF-8")
    }
  implicit def deser[A](implicit dec: Reader[A]): Deserializer[A] =
    new Deserializer[A] {
      def apply(b: Array[Byte]) = upickle.default.read[A](new String(b))
    }

  implicit val sharedFileRW =
    upickle.default.macroRW[tasks.fileservice.SharedFile]

}
