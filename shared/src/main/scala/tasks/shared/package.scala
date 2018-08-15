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
}
