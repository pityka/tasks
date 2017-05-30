package tasks

import io.circe.generic.semiauto._
import io.circe._
import cats.syntax.either._
import akka.actor._

import java.io.File

package object wire {

  implicit val actorRefEncoder: Encoder[ActorRef] =
    Encoder.encodeString.contramap[ActorRef](ar =>
      akka.serialization.Serialization.serializedActorPath(ar))
  implicit def actorRefDecoder(
      implicit as: ExtendedActorSystem): Decoder[ActorRef] =
    Decoder.decodeString.emap(
      str =>
        Either
          .catchNonFatal(as.provider.resolveActorRef(str))
          .leftMap(t => "ActorRef"))

  implicit val throwableEncoder: Encoder[Throwable] =
    Encoder.encodeString.contramap[Throwable](_.getMessage)
  implicit val throwableDecoder
    : Decoder[Throwable] = Decoder.decodeString.emap(str =>
    Either.catchNonFatal(new RuntimeException(str)).leftMap(t => "Throwable"))

  implicit val fileEncoder: Encoder[File] =
    Encoder.encodeString.contramap[File](_.getAbsolutePath)
  implicit val fileDecoder: Decoder[File] = Decoder.decodeString.emap(str =>
    Either.catchNonFatal(new File(str)).leftMap(t => "File"))

}
