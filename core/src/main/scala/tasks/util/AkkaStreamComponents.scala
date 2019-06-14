// package tasks.util

// import akka.stream.scaladsl.{Flow, FileIO, Compression, Sink, Keep}
// import akka.util.ByteString
// import java.io.File
// import scala.concurrent.duration._
// import scala.concurrent.{ExecutionContext, Future}

// object AkkaStreamComponents {

//   val decomposeByteString =
//     Flow[ByteString].mapConcat(bs => bs.asByteBuffers.map(ByteString(_)).toList)

//   def gunzip(maxBytesPerChunk: Int = 65536) =
//     decomposeByteString
//       .via(Compression.gunzip(maxBytesPerChunk))

//   def gzip(level: Int, blockSize: Long) = {
//     def gzip(data: ByteString) = {
//       val bos = new java.io.ByteArrayOutputStream(data.length)
//       val gzip = new java.util.zip.GZIPOutputStream(bos) {
//         `def`.setLevel(level)
//       }
//       val writeableChannel = java.nio.channels.Channels.newChannel(gzip)
//       data.asByteBuffers.foreach(writeableChannel.write)

//       gzip.close
//       val compressed = bos.toByteArray
//       bos.close()
//       ByteString(compressed)
//     }

//     Flow[ByteString]
//       .via(
//         strictBatchWeighted[ByteString, ByteString](blockSize, _.size.toLong)(
//           reduceByteStrings))
//       .map(gzip)
//   }

//   val reduceByteStrings = (s: Seq[ByteString]) => {
//     val builder = ByteString.newBuilder
//     s.foreach(b => builder.append(b))
//     builder.result
//   }
//   // def parallelize[T, K](parallelism: Int, bufferSize: Int = 1000)(
//   //     f: T => scala.collection.immutable.Iterable[K])(
//   //     implicit
//   //     ec: ExecutionContext): Flow[T, K, akka.NotUsed] =
//   //   if (parallelism == 1)
//   //     Flow[T].mapConcat(f)
//   //   else
//   //     Flow[T]
//   //       .grouped(bufferSize)
//   //       .mapAsync(parallelism) { lines =>
//   //         Future {
//   //           lines.map(f)
//   //         }(ec)
//   //       }
//   //       .mapConcat(_.flatten)

//   def strictBatchWeighted[T, K](
//       maxWeight: Long,
//       cost: T => Long,
//       maxDuration: FiniteDuration = 1 seconds
//   )(reduce: Seq[T] => K): Flow[T, K, akka.NotUsed] =
//     Flow[T]
//       .groupedWeightedWithin(maxWeight, maxDuration)(cost)
//       .map(reduce)

//   def strictBatchWeightedByteStrings(
//       maxWeight: Long,
//       cost: ByteString => Long,
//       maxDuration: FiniteDuration = 1 seconds
//   ): Flow[ByteString, ByteString, akka.NotUsed] =
//     strictBatchWeighted[ByteString, ByteString](maxWeight, cost, maxDuration)(
//       reduceByteStrings)

//   def delimiter(
//       delimiter: Byte,
//       maximumFrameLength: Int): Flow[ByteString, ByteString, akka.NotUsed] =
//     Flow[ByteString].statefulMapConcat { () =>
//       var buffer = ByteString.empty

//       def indexOfDelimiterIn(bs: ByteString): Int = {
//         var i = 0
//         var idx = -1
//         while (idx == -1 && i < bs.length) {
//           if (bs.apply(i) == delimiter) {
//             idx = i
//           }
//           i += 1
//         }
//         idx
//       }

//       val empty = ByteString("")

//       { elem =>
//         var idx = indexOfDelimiterIn(elem)
//         if (idx == -1) {
//           if (buffer.size + elem.size > maximumFrameLength) {
//             throw new RuntimeException("Buffer too small")
//           }
//           buffer = buffer ++ elem
//           Nil
//         } else {
//           val output = scala.collection.mutable.ArrayBuffer[ByteString]()
//           var suffix = elem
//           while (idx != -1) {

//             val prefix = suffix.take(idx)
//             val nextOutput = buffer ++ prefix
//             buffer = empty
//             output.append(nextOutput)

//             suffix = suffix.drop(idx + 1)
//             idx = indexOfDelimiterIn(suffix)
//           }

//           if (suffix.size > maximumFrameLength) {
//             throw new RuntimeException("Buffer too small")
//           }
//           buffer = suffix
//           output.toList
//         }
//       }

//     }
// }
