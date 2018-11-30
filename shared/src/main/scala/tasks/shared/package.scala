package tasks

import shapeless.tag.@@
import shapeless.tag
import io.circe.{Encoder, Decoder}

package object shared {

  trait CodeVersionTag

  type CodeVersion = String @@ CodeVersionTag
  def CodeVersion(s: String): CodeVersion = tag[CodeVersionTag][String](s)

  implicit val codeVersionEncoder: Encoder[CodeVersion] =
    Encoder.encodeString.contramap(identity)
  implicit val codeVersionDecoder: Decoder[CodeVersion] =
    Decoder.decodeString.map(CodeVersion(_))


  trait PriorityTag
  type Priority = Int @@ PriorityTag
  def Priority(s: Int): Priority = tag[PriorityTag][Int](s)
  implicit val priorityEncoder: Encoder[Priority] =
    Encoder.encodeInt.contramap(identity)
  implicit val priorityDecoder: Decoder[Priority] =
    Decoder.decodeInt.map(Priority(_))
}
