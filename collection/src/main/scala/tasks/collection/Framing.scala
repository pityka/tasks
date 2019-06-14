package tasks.ecoll

import tasks.queue._
import tasks._
import tasks.util.rightOrThrow
import akka.stream.scaladsl._
import akka.util.ByteString
import scala.concurrent.ExecutionContext

trait Framing { self: Constants =>
  def decodeFrame =
    Flow[ByteString]
      .via(
        lame.Framing.delimiter(Eol.head,
                               maximumFrameLength = Int.MaxValue,
                               allowTruncation = true))

  def decodeFileForFlatJoin[T](
      decoder: Deserializer[T],
      stringKey: flatjoin.StringKey[T],
      parallelism: Int
  )(
      ecoll: SharedFile
  )(
      implicit ec: ExecutionContext,
      tsc: TaskSystemComponents
  ): Source[(String, T), _] =
    lame.BlockGunzip
      .sourceFromFactory(virtualFilePointer = 0L, customInflater = None) { _ =>
        ecoll.source
      }
      .via(decodeFrame)
      .via(
        lame.Parallel
          .mapAsync[ByteString, (String, T)](
            parallelism,
            ElemBufferSize
          ) { line =>
            val t = rightOrThrow(decoder.apply(line.toArray))
            val k = stringKey.key(t)
            (k, t)
          }
      )

}
