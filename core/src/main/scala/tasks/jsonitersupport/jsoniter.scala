package tasks

import tasks.queue._
import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.google.common.{hash => ghash}
import com.google.common.io.ByteStreams

package object jsonitersupport {

  implicit def ser[A: JsonValueCodec]: Serializer[A] =
    new Serializer[A] {
      def hash(a: A): String = {
        val os = new ghash.HashingOutputStream(
          ghash.Hashing.murmur3_128,
          ByteStreams.nullOutputStream()
        )
        writeToStream(a, os)
        os.hash().toString()
      }

      def apply(a: A) = writeToArray(a)
    }
  implicit def deser[A: JsonValueCodec]: Deserializer[A] =
    new Deserializer[A] {

      def apply(in: Array[Byte]) =
        scala.util
          .Try(readFromArray[A](in))
          .toEither
          .left
          .map(e => e.toString + "\n" + e.getStackTrace.mkString(";\n"))
    }

  implicit val sharedFileCodec: JsonValueCodec[SharedFile] =
    JsonCodecMaker.make[SharedFile]

  // standard types

  implicit val stringCodec: JsonValueCodec[String] =
    JsonCodecMaker.make
  implicit val intCodec: JsonValueCodec[Int] =
    JsonCodecMaker.make
  implicit val doubleCodec: JsonValueCodec[Double] =
    JsonCodecMaker.make
  implicit val floatCodec: JsonValueCodec[Float] =
    JsonCodecMaker.make
  implicit val charCodec: JsonValueCodec[Char] =
    JsonCodecMaker.make
  implicit val byteCodec: JsonValueCodec[Byte] =
    JsonCodecMaker.make
  implicit val shortCodec: JsonValueCodec[Short] =
    JsonCodecMaker.make
  implicit val longCodec: JsonValueCodec[Long] =
    JsonCodecMaker.make
  implicit val booleanCodec: JsonValueCodec[Boolean] =
    JsonCodecMaker.make
  implicit def traversableCodec[A](implicit
      c: JsonValueCodec[A]
  ): JsonValueCodec[Iterable[A]] = {
    val _ = c
    JsonCodecMaker.make
  }
  implicit def tuple2Codec[A1, A2](implicit
      c1: JsonValueCodec[A1],
      c2: JsonValueCodec[A2]
  ): JsonValueCodec[(A1, A2)] = {
    val _ = (c1, c2)
    JsonCodecMaker.make
  }
  implicit def tuple3Codec[A1, A2, A3](implicit
      c1: JsonValueCodec[A1],
      c2: JsonValueCodec[A2],
      c3: JsonValueCodec[A3]
  ): JsonValueCodec[(A1, A2, A3)] = {
    val _ = (c1, c2, c3)
    JsonCodecMaker.make
  }

  // implicit val serdeSharedFile = tasks.makeSerDe[SharedFile]

}
