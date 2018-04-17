package tasks.util

import akka.stream.scaladsl.Flow
import akka.util.ByteString
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object AkkaStreamComponents {

  def parallelize[T, K](parallelism: Int, bufferSize: Int = 1000)(f: T => K)(
      implicit
      ec: ExecutionContext): Flow[T, K, _] =
    if (parallelism == 1)
      Flow[T].map(f)
    else
      Flow[T]
        .grouped(bufferSize)
        .mapAsync(parallelism) { lines =>
          Future {
            lines.map(f)
          }(ec)
        }
        .mapConcat(identity)

  def strictBatchWeighted[T](
      maxWeight: Long,
      cost: T => Long,
      maxDuration: FiniteDuration = 1 seconds
  )(reduce: (T, T) => T): Flow[T, T, _] =
    Flow[T]
      .groupedWeightedWithin(maxWeight, maxDuration)(cost)
      .map(_.reduce(reduce))

  def delimiter(
      delimiter: Byte,
      maximumFrameLength: Int): Flow[ByteString, ByteString, akka.NotUsed] =
    Flow[ByteString].statefulMapConcat { () =>
      var buffer = ByteString.empty

      def indexOfDelimiterIn(bs: ByteString): Int = {
        var i = 0
        var idx = -1
        while (idx == -1 && i < bs.length) {
          if (bs.apply(i) == delimiter) {
            idx = i
          }
          i += 1
        }
        idx
      }

      val empty = ByteString("")

      { elem =>
        var idx = indexOfDelimiterIn(elem)
        if (idx == -1) {
          if (buffer.size + elem.size > maximumFrameLength) {
            throw new RuntimeException("Buffer too small")
          }
          buffer = buffer ++ elem
          Nil
        } else {
          val output = scala.collection.mutable.ArrayBuffer[ByteString]()
          var suffix = elem
          while (idx != -1) {

            val prefix = suffix.take(idx)
            val nextOutput = buffer ++ prefix
            buffer = empty
            output.append(nextOutput)

            suffix = suffix.drop(idx + 1)
            idx = indexOfDelimiterIn(suffix)
          }

          if (suffix.size > maximumFrameLength) {
            throw new RuntimeException("Buffer too small")
          }
          buffer = suffix
          output.toList
        }
      }

    }
}
