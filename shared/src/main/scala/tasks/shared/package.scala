package tasks

import shapeless.tag.@@
import shapeless.tag
import com.github.plokhotnyuk.jsoniter_scala.core._

package object shared {

  trait CodeVersionTag

  type CodeVersion = String @@ CodeVersionTag
  def CodeVersion(s: String): CodeVersion = tag[CodeVersionTag][String](s)

  implicit val codeVersionEncoder: JsonValueCodec[CodeVersion] =
    new JsonValueCodec[CodeVersion] {
      val nullValue: CodeVersion = null.asInstanceOf[CodeVersion]

      def encodeValue(x: CodeVersion, out: JsonWriter): _root_.scala.Unit =
        out.writeVal(x)

      def decodeValue(in: JsonReader, default: CodeVersion): CodeVersion =
        CodeVersion(in.readString(default))
    }

  trait PriorityTag
  type Priority = Int @@ PriorityTag
  def Priority(s: Int): Priority = tag[PriorityTag][Int](s)

  implicit val priorityCodec: JsonValueCodec[Priority] =
    new JsonValueCodec[Priority] {
      val nullValue: Priority = null.asInstanceOf[Priority]

      def encodeValue(x: Priority, out: JsonWriter): _root_.scala.Unit =
        out.writeVal(x)

      def decodeValue(in: JsonReader, default: Priority): Priority =
        Priority(in.readInt())
    }

  trait ElapsedTimeTag
  type ElapsedTimeNanoSeconds = Long @@ ElapsedTimeTag
  def ElapsedTimeNanoSeconds(s: Long): ElapsedTimeNanoSeconds =
    tag[ElapsedTimeTag][Long](s)

  implicit val elapsedTimeCodec: JsonValueCodec[ElapsedTimeNanoSeconds] =
    new JsonValueCodec[ElapsedTimeNanoSeconds] {
      val nullValue: ElapsedTimeNanoSeconds =
        null.asInstanceOf[ElapsedTimeNanoSeconds]

      def encodeValue(
          x: ElapsedTimeNanoSeconds,
          out: JsonWriter
      ): _root_.scala.Unit =
        out.writeVal(x)

      def decodeValue(
          in: JsonReader,
          default: ElapsedTimeNanoSeconds
      ): ElapsedTimeNanoSeconds =
        ElapsedTimeNanoSeconds(in.readLong())
    }
}
