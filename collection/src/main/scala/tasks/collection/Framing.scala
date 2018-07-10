package tasks.collection

import tasks.queue._
import tasks._
import tasks.util.AkkaStreamComponents
import akka.stream.scaladsl._
import akka.util.ByteString
import scala.concurrent.ExecutionContext

trait Framing { self: Constants =>
  def decodeFrame =
    Flow[ByteString]
      .via(AkkaStreamComponents
        .strictBatchWeighted[ByteString](BufferSize, _.size.toLong)(_ ++ _))
      .via(Compression.gunzip())
      .via(AkkaStreamComponents.delimiter(Eof.head,
                                          maximumFrameLength = Int.MaxValue))

  def decodeFileForFlatJoin[T](decoder: Deserializer[T],
                               stringKey: flatjoin.StringKey[T],
                               parallelism: Int)(file: java.io.File)(
      implicit ec: ExecutionContext): Source[(String, ByteString), _] =
    FileIO
      .fromPath(file.toPath)
      .via(decodeFrame)
      .via(
        AkkaStreamComponents
          .parallelize[ByteString, (String, ByteString)](parallelism,
                                                         ElemBufferSize) {
            line =>
              val t = decoder.apply(line.toArray)
              val k = stringKey.key(t)
              List((k, line))
          })

}
