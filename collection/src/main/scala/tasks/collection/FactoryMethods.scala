package tasks.collection

import tasks.queue._
import tasks._
import tasks.util.AkkaStreamComponents
import akka.stream.scaladsl._
import akka.util.ByteString
import scala.concurrent.Future
import java.io._

trait FactoryMethods { self: Constants =>

  def fromSource[T](source: Source[T, _],
                    name: String,
                    partitionSize: Long = Long.MaxValue,
                    parallelism: Int = 1)(
      implicit encoder: Serializer[T],
      tsc: TaskSystemComponents
  ): Future[EColl[T]] = {
    implicit val mat = tsc.actorMaterializer
    implicit val ec = mat.executionContext

    def gzip(data: Array[Byte]) = {
      val bos = new ByteArrayOutputStream(data.length)
      val gzip = new java.util.zip.GZIPOutputStream(bos)
      gzip.write(data);
      gzip.close
      val compressed = bos.toByteArray
      bos.close()
      compressed
    }

    val gzipPar = if (parallelism == 1) 1 else parallelism / 2
    val encoderPar = if (parallelism == 1) 1 else parallelism - gzipPar

    val gzipFlow =
      if (gzipPar == 1)
        Flow[ByteString].map(bs => ByteString(gzip(bs.toArray)))
      else
        Flow[ByteString].mapAsync(gzipPar)(bs =>
          Future(ByteString(gzip(bs.toArray))))

    val sink = {
      val dedicatedDispatcher =
        tsc.actorsystem.dispatchers.lookup("task-dedicated-io")

      def createSharedFile(file: File, part: Int) =
        SharedFile(file, name = name + ".part." + part, deleteFile = true)(
          tsc.withChildPrefix(name + ".ecoll")).map(f => part -> f)

      def write(os: OutputStream, bs: ByteString): Future[Unit] =
        Future {
          val ba = bs.toArray
          os.write(ba, 0, ba.size)
        }(dedicatedDispatcher)

      val unbufferedSink = Sink
        .foldAsync[(Long,
                    Int,
                    Option[(OutputStream, File)],
                    List[Future[(Int, SharedFile)]]),
                   ByteString]((partitionSize, -1, None, Nil)) {
          case ((counter, partCount, os, files), elem) =>
            if (elem.size >= partitionSize - counter) {
              val f =
                File.createTempFile("part_" + (partCount + 1) + "_", "part")
              f.deleteOnExit

              val currentOS = new java.io.FileOutputStream(f)
              val closeFileAndStartUpload = Future(os.map {
                case (os, file) =>
                  os.close
                  createSharedFile(file, partCount)
              })(dedicatedDispatcher)

              for {
                sf <- closeFileAndStartUpload
                _ <- write(currentOS, elem)
              } yield {
                (elem.size.toLong,
                 partCount + 1,
                 Some(currentOS -> f),
                 sf.toList ::: files)
              }
            } else {
              write(os.get._1, elem).map(_ =>
                (elem.size.toLong + counter, partCount, os, files))
            }
        }
        .mapMaterializedValue(foldedFuture =>
          for {
            (_, partCount, os, files) <- foldedFuture
            lastPartition <- Future(os.map {
              case (os, file) =>
                os.close
                createSharedFile(file, partCount)
            })(dedicatedDispatcher)
            partitions <- (Future
              .sequence(lastPartition.toList ::: files))
              .map(_.sortBy(_._1).map(_._2))
          } yield partitions)

      Flow[ByteString]
        .via(AkkaStreamComponents
          .strictBatchWeighted[ByteString](BufferSize, _.size.toLong)(_ ++ _))
        .async
        .toMat(unbufferedSink)(Keep.right)
    }

    val encoderFlow =
      AkkaStreamComponents
        .parallelize[T, ByteString](encoderPar, ElemBufferSize)(elem =>
          List(ByteString(encoder.apply(elem)) ++ Eof))

    var count = 0L
    source
      .map {
        case elem =>
          count += 1
          elem
      }
      .via(encoderFlow)
      .via(AkkaStreamComponents
        .strictBatchWeighted[ByteString](BufferSize, _.size.toLong)(_ ++ _))
      .via(gzipFlow)
      .runWith(
        sink
          .mapMaterializedValue(
            _.flatMap { sfs =>
              EColl[T](sfs.toList, count).writeLength(name)
            }
          )
      )

  }

  def fromSourceAsPartition[T](source: Source[T, _], name: String, idx: Int)(
      implicit encoder: Serializer[T],
      tsc: TaskSystemComponents): Future[EColl[T]] = {
    var count = 0L
    val s2 =
      source
        .map { x =>
          count += 1
          x
        }
        .map(t => ByteString(encoder.apply(t)) ++ Eof)
        .via(AkkaStreamComponents
          .strictBatchWeighted[ByteString](512 * 1024, _.size.toLong)(_ ++ _))
        .via(Compression.gzip)

    SharedFile(s2, name + ".part." + idx)(tsc.withChildPrefix(name + ".ecoll"))
      .map(sf => EColl[T](sf :: Nil, count))(tsc.executionContext)

  }

}
