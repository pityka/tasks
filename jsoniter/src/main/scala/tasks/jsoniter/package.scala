package tasks

import tasks.queue._
import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros._

package object jsonitersupport {

  implicit def ser[A: JsonValueCodec]: Serializer[A] =
    new Serializer[A] {
      def apply(a: A) = writeToArray(a)
    }
  implicit def deser[A: JsonValueCodec]: Deserializer[A] =
    new Deserializer[A] {
      def apply(b: Array[Byte]) =
        scala.util.Try(readFromArray[A](b)).toEither.left.map(_.toString)
    }

  implicit val mPathCodec =
    JsonCodecMaker.make[fileservice.ManagedFilePath](CodecMakerConfig())

  implicit val rPathCodec =
    JsonCodecMaker.make[fileservice.RemoteFilePath](CodecMakerConfig())

  implicit val fPathCodec =
    JsonCodecMaker.make[fileservice.FilePath](CodecMakerConfig())

  implicit val sharedFileCodec: JsonValueCodec[SharedFile] =
    JsonCodecMaker.make[SharedFile](CodecMakerConfig())

}
