package tasks.collection

import flatjoin._
import tasks.queue._
import tasks._
import tasks.util.AkkaStreamComponents
import java.io.File
import akka.stream.scaladsl._
import akka.util.ByteString
import scala.concurrent.{Future, ExecutionContext}
import java.nio._
import java.io._

case class EColl[T](partitions: List[SharedFile], length: Long)
    extends ResultWithSharedFiles(partitions: _*) {

  def basename: String =
    partitions.head.name.split(".part\\.").dropRight(1).mkString(".part.")

  def ++(that: EColl[T]): EColl[T] =
    EColl[T](partitions ++ that.partitions, length + that.length)

  def writeLength(name: String)(implicit tsc: TaskSystemComponents) = {
    import tsc.actorMaterializer.executionContext

    SharedFile(Source.single(ByteString(length.toString)),
               name = name + ".length")(tsc.withChildPrefix(name + ".ecoll"))
      .map(_ => this)
  }

  def source(parallelism: Int)(implicit decoder: Deserializer[T],
                               tsc: TaskSystemComponents): Source[T, _] = {
    val decoderFlow =
      AkkaStreamComponents
        .parallelize[ByteString, T](parallelism, EColl.ElemBufferSize)(line =>
          List(decoder(line.toArray)))(tsc.actorMaterializer.executionContext)

    Source(partitions)
      .flatMapConcat(
        _.source
          .via(EColl.decodeFrame)
          .via(decoderFlow)
      )
  }

  def sourceOfPartition(i: Int)(implicit decoder: Deserializer[T],
                                tsc: TaskSystemComponents): Source[T, _] =
    partitions(i).source
      .via(EColl.decodeFrame)
      .map(line => decoder.apply(line.toArray))

}

object EColl {
  val ElemBufferSize = 256
  val BufferSize = 1024L * 512
  private val eof = ByteString("\n")

  val decodeFrame = Flow[ByteString]
    .via(AkkaStreamComponents
      .strictBatchWeighted[ByteString](EColl.BufferSize, _.size.toLong)(_ ++ _))
    .via(Compression.gunzip())
    .via(AkkaStreamComponents.delimiter(EColl.eof.head,
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
                                                         EColl.ElemBufferSize) {
            line =>
              val t = decoder.apply(line.toArray)
              val k = stringKey.key(t)
              List((k, line))
          })

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
        .parallelize[T, ByteString](encoderPar, EColl.ElemBufferSize)(elem =>
          List(ByteString(encoder.apply(elem)) ++ eof))

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
        .map(t => ByteString(encoder.apply(t)) ++ eof)
        .via(AkkaStreamComponents
          .strictBatchWeighted[ByteString](512 * 1024, _.size.toLong)(_ ++ _))
        .via(Compression.gzip)

    SharedFile(s2, name + ".part." + idx)(tsc.withChildPrefix(name + ".ecoll"))
      .map(sf => EColl[T](sf :: Nil, count))(tsc.executionContext)

  }

  import scala.language.experimental.macros

  def repartition[A](taskID: String, taskVersion: Int)(
      partitionSize: Long): TaskDefinition[EColl[A], EColl[A]] =
    macro Macros
      .repartitionMacro[A]

  def map[A, B](taskID: String, taskVersion: Int)(
      fun: A => B): TaskDefinition[EColl[A], EColl[B]] =
    macro Macros
      .mapMacro[A, B]

  def collect[A, B](taskID: String, taskVersion: Int)(
      fun: PartialFunction[A, B]): TaskDefinition[EColl[A], EColl[B]] =
    macro Macros
      .collectMacro[A, B]

  def scan[A, B, C](taskID: String, taskVersion: Int)(nameFun: B => String)(
      partitionSize: Long,
      prepareB: B => ComputationEnvironment => Future[C],
      fun: (C, A) => C): TaskDefinition[(EColl[A], B), EColl[C]] =
    macro Macros
      .scanMacro[A, B, C]

  def mapSourceWith[A, B, C](taskID: String, taskVersion: Int)(
      nameFun: B => String)(fun: (Source[A, _], B) => ComputationEnvironment => Source[
                              C,
                              _]): TaskDefinition[(EColl[A], B), EColl[C]] =
    macro Macros
      .mapSourceWithMacro[A, B, C]

  def mapFullSourceWith[A, B, C](taskID: String, taskVersion: Int)(
      nameFun: B => String)(partitionSize: Long,
                            fun: (Source[A, _], B) => ComputationEnvironment => Source[
                              C,
                              _]): TaskDefinition[(EColl[A], B), EColl[C]] =
    macro Macros
      .mapFullSourceWithMacro[A, B, C]

  def mapElemWith[A, B, C, D](taskID: String, taskVersion: Int)(
      fun1: B => ComputationEnvironment => Future[C])(
      fun2: (A, C) => D): TaskDefinition[(EColl[A], B), EColl[D]] =
    macro Macros
      .mapElemWithMacro[A, B, C, D]

  def filter[A](taskID: String, taskVersion: Int)(
      fun: A => Boolean): TaskDefinition[EColl[A], EColl[A]] =
    macro Macros
      .filterMacro[A]

  def mapConcat[A, B](taskID: String, taskVersion: Int)(
      fun: A => Iterable[B]): TaskDefinition[EColl[A], EColl[B]] =
    macro Macros
      .mapConcatMacro[A, B]

  def sortBy[A](taskID: String, taskVersion: Int)(
      partitionSize: Long,
      fun: A => String): TaskDefinition[EColl[A], EColl[A]] =
    macro Macros
      .sortByMacro[A]

  def groupBy[A](taskID: String, taskVersion: Int)(
      partitionSize: Long,
      fun: A => String,
      maxParallelJoins: Option[Int]): TaskDefinition[EColl[A], EColl[Seq[A]]] =
    macro Macros
      .groupByMacro[A]

  def groupBySorted[A](taskID: String, taskVersion: Int)(
      partitionSize: Long,
      fun: A => String): TaskDefinition[EColl[A], EColl[Seq[A]]] =
    macro Macros
      .groupBySortedMacro[A]

  def outerJoinBy[A](taskID: String, taskVersion: Int)(
      partitionSize: Long,
      fun: A => String,
      maxParallelJoins: Option[Int]): TaskDefinition[List[EColl[A]],
                                                     EColl[Seq[Option[A]]]] =
    macro Macros.outerJoinByMacro[A]

  def outerJoinBySorted[A](taskID: String, taskVersion: Int)(
      partitionSize: Long,
      fun: A => String): TaskDefinition[List[EColl[A]], EColl[Seq[Option[A]]]] =
    macro Macros.outerJoinBySortedMacro[A]

  def innerJoinBy2[A, B](taskID: String, taskVersion: Int)(
      partitionSize: Long,
      funA: A => String,
      funB: B => String,
      maxParallelJoins: Option[Int]): TaskDefinition[(EColl[A], EColl[B]),
                                                     EColl[(A, B)]] =
    macro Macros.innerJoinBy2Macro[A, B]

  def outerJoinBy2[A, B](taskID: String, taskVersion: Int)(
      partitionSize: Long,
      funA: A => String,
      funB: B => String,
      maxParallelJoins: Option[Int]): TaskDefinition[(EColl[A], EColl[B]),
                                                     EColl[(Option[A],
                                                            Option[B])]] =
    macro Macros.outerJoinBy2Macro[A, B]

  def outerJoinBy2Sorted[A, B](taskID: String, taskVersion: Int)(
      partitionSize: Long,
      funA: A => String,
      funB: B => String): TaskDefinition[(EColl[A], EColl[B]),
                                         EColl[(Option[A], Option[B])]] =
    macro Macros.outerJoinBy2SortedMacro[A, B]

  def foldLeft[A, B](taskID: String, taskVersion: Int)(
      zero: B,
      fun: (B, A) => B): TaskDefinition[EColl[A], B] =
    macro Macros
      .foldLeftMacro[A, B]

  def foldLeftWith[A, B, C, D](taskID: String, taskVersion: Int)(
      zero: B,
      fun1: C => ComputationEnvironment => Future[D],
      fun2: (B, A, D) => B): TaskDefinition[(EColl[A], C), EColl[B]] =
    macro Macros
      .foldLeftWithMacro[A, B, C, D]

  def take[A](taskID: String, taskVersion: Int)(
      partitionSize: Long): TaskDefinition[(EColl[A], Int), EColl[A]] =
    macro Macros.takeMacro[A]

  def uniqueSorted[A](taskID: String, taskVersion: Int)(
      partitionSize: Long): TaskDefinition[EColl[A], EColl[A]] =
    macro Macros.uniqueSortedMacro[A]

  def toSeq[A](taskID: String,
               taskVersion: Int): TaskDefinition[EColl[A], Seq[A]] =
    macro Macros.toSeqMacro[A]

  def reduce[A](taskID: String, taskVersion: Int)(
      fun: (A, A) => A): TaskDefinition[EColl[A], A] =
    macro Macros
      .reduceMacro[A]

  def reduceSeq[A](taskID: String, taskVersion: Int)(
      fun: Seq[A] => A): TaskDefinition[EColl[Seq[A]], EColl[A]] =
    macro Macros
      .reduceSeqMacro[A]

  implicit def flatJoinFormat[T: Deserializer: Serializer] = new Format[T] {
    def toBytes(t: T): ByteBuffer =
      ByteBuffer.wrap(implicitly[Serializer[T]].apply(t))
    def fromBytes(bb: ByteBuffer): T = {
      val ba = ByteBuffer.allocate(bb.remaining)
      while (ba.hasRemaining) {
        ba.put(bb.get)
      }
      implicitly[Deserializer[T]].apply(ba.array)
    }
  }

}
