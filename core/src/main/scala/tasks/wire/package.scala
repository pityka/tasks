package tasks

import io.circe._
import cats.syntax.either._
import akka.actor._

import java.io.File

package object wire {

  implicit val actorRefEncoder: Encoder[ActorRef] =
    Encoder.encodeString.contramap[ActorRef](
      ar => akka.serialization.Serialization.serializedActorPath(ar)
    )
  implicit def actorRefDecoder(
      implicit as: ExtendedActorSystem
  ): Decoder[ActorRef] =
    Decoder.decodeString.emap(
      str =>
        Either
          .catchNonFatal(as.provider.resolveActorRef(str))
          .leftMap(_ => "ActorRef")
    )

  implicit val throwableEncoder: Encoder[Throwable] =
    Encoder
      .encodeTuple2[String, List[(String, String, String, Int)]]
      .contramap[Throwable] { throwable =>
        (
          throwable.getMessage,
          throwable.getStackTrace.toList.map(
            stackTraceElement =>
              (
                stackTraceElement.getClassName,
                stackTraceElement.getMethodName,
                stackTraceElement.getFileName,
                stackTraceElement.getLineNumber
              )
          )
        )
      }
  implicit val throwableDecoder: Decoder[Throwable] =
    Decoder.decodeTuple2[String, List[(String, String, String, Int)]].emap {
      case (msg, stackTrace) =>
        val exc = new Exception(msg)
        exc.setStackTrace(stackTrace.map {
          case (cls, method, file, line) =>
            new java.lang.StackTraceElement(cls, method, file, line)
        }.toArray)
        Either
          .catchNonFatal(exc)
          .leftMap(_ => "Throwable")
    }

  implicit val fileEncoder: Encoder[File] =
    Encoder.encodeString.contramap[File](_.getAbsolutePath)
  implicit val fileDecoder: Decoder[File] = Decoder.decodeString.emap(
    str => Either.catchNonFatal(new File(str)).leftMap(_ => "File")
  )

}
