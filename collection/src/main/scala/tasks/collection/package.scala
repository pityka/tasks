package tasks.collection

import flatjoin._
import tasks.queue._
import tasks._
import java.io.File
import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import scala.concurrent.Future
import java.nio._

case class EColl[T](partitions: List[SharedFile])
    extends ResultWithSharedFiles(partitions: _*) {

  def basename: String =
    partitions.head.name.split(".part\\.").dropRight(1).mkString(".part.")

  def ++(that: EColl[T]): EColl[T] = EColl[T](partitions ++ that.partitions)

  def source(implicit decoder: Deserializer[T],
             tsc: TaskSystemComponents): Source[T, _] =
    Source(partitions).flatMapConcat(
      _.source
        .via(Compression.gunzip())
        .via(Framing.delimiter(ByteString("\n"),
                               maximumFrameLength = Int.MaxValue,
                               allowTruncation = false))
        .map(line => decoder(line.toArray)))

  def source(i: Int)(implicit decoder: Deserializer[T],
                     tsc: TaskSystemComponents): Source[T, _] =
    partitions(i).source
      .via(Compression.gunzip())
      .via(
        Framing.delimiter(ByteString("\n"),
                          maximumFrameLength = Int.MaxValue,
                          allowTruncation = false))
      .map(line => decoder.apply(line.toArray))

}

object EColl {

  private val eof = ByteString("\n")

  def partitionsFromSource[T: StringKey](source: Source[T, _],
                                         name: String,
                                         partitions: Int = 128)(
      implicit encoder: Serializer[T],
      tsc: TaskSystemComponents
  ) = {
    implicit val ec = tsc.executionContext
    implicit val mat = tsc.actorMaterializer

    def sinkToFlow[I, U](sink: Sink[I, U]): Flow[I, U, U] =
      Flow.fromGraph(GraphDSL.create(sink) { implicit builder => sink =>
        FlowShape.of[I, U](sink.in, builder.materializedValue)
      })

    val shardFlow: Flow[T, Seq[File], NotUsed] = {

      val files = 0 until partitions map { i =>
        val file = File.createTempFile("shard_" + i + "_", "shard")
        file.deleteOnExit
        (i, file)
      } toMap

      val hash = (t: T) =>
        math.abs(
          scala.util.hashing.MurmurHash3
            .stringHash(implicitly[StringKey[T]].key(t)) % partitions)

      val flows: List[(Int, Flow[(ByteString, Int), Seq[File], _])] =
        files.map {
          case (idx, file) =>
            idx -> sinkToFlow(
              Flow[(ByteString, Int)]
                .map(_._1 ++ eof)
                .batchWeighted(512 * 1024, _.size.toLong, identity)(_ ++ _)
                .via(Compression.gzip)
                .toMat(FileIO.toPath(file.toPath))(Keep.right)
                .mapMaterializedValue(_.map(_ => file :: Nil)))
              .mapAsync(1)(x => x)

        }.toList

      val shardFlow: Flow[(ByteString, Int), Seq[File], NotUsed] = Flow
        .fromGraph(
          GraphDSL.create() { implicit b =>
            import GraphDSL.Implicits._
            val broadcast =
              b.add(Partition[(ByteString, Int)](flows.size, _._2))
            val merge = b.add(Merge[Seq[File]](flows.size))
            flows.foreach {
              case (i, f) =>
                val flowShape = b.add(f)
                broadcast.out(i) ~> flowShape.in
                flowShape.out ~> merge.in(i)
            }

            new FlowShape(broadcast.in, merge.out)
          }
        )
        .reduce(_ ++ _)

      Flow[T]
        .map {
          case elem =>
            ByteString(encoder.apply(elem)) -> hash(elem)
        }
        .viaMat(shardFlow)(Keep.right)
    }

    var count = 0L

    source
      .map { x =>
        count += 1
        x
      }
      .via(shardFlow)
      .mapAsync(1) { files =>
        Source(files.zipWithIndex.toList)
          .mapAsync(1)(file =>
            SharedFile(file._1,
                       name = name + ".part." + file._2,
                       deleteFile = true)(tsc.withChildPrefix(name + ".ecoll")))
          .runWith(Sink.seq)
      }
      .mapAsync(1) { sfs =>
        SharedFile(Source.single(ByteString(count.toString)),
                   name = name + ".length")(
          tsc.withChildPrefix(name + ".ecoll")).map(_ => sfs)
      }
      .map { sfs =>
        EColl[T](sfs.toList)
      }
      .runWith(Sink.head)
  }

  def fromSource[T](source: Source[T, _],
                    name: String,
                    asPartitionOf: Option[Int] = None)(
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
        .batchWeighted(512 * 1024, _.size.toLong, identity)(_ ++ _)
        .via(Compression.gzip)

    val ecoll = asPartitionOf match {
      case None =>
        SharedFile(s2, name + ".part.0")(tsc.withChildPrefix(name + ".ecoll"))
          .map(sf => EColl[T](sf :: Nil))(tsc.executionContext)
      case Some(idx) =>
        SharedFile(s2, name + ".part." + idx)(
          tsc.withChildPrefix(name + ".ecoll"))
          .map(sf => EColl[T](sf :: Nil))(tsc.executionContext)
    }
    import tsc.actorMaterializer.executionContext
    ecoll.flatMap { ecoll =>
      SharedFile(Source.single(ByteString(count.toString)),
                 name = name + ".length")(tsc.withChildPrefix(name + ".ecoll"))
        .map(_ => ecoll)

    }

  }

  import scala.language.experimental.macros

  def map[A, B](taskID: String, taskVersion: Int)(
      fun: A => B): TaskDefinition[EColl[A], EColl[B]] =
    macro Macros
      .mapMacro[A, B]

  def collect[A, B](taskID: String, taskVersion: Int)(
      fun: PartialFunction[A, B]): TaskDefinition[EColl[A], EColl[B]] =
    macro Macros
      .collectMacro[A, B]

  def mapSourceWith[A, B, C](
      taskID: String,
      taskVersion: Int)(fun: (Source[A, _], B) => ComputationEnvironment => Source[
                          C,
                          _]): TaskDefinition[(EColl[A], B), EColl[C]] =
    macro Macros
      .mapSourceWithMacro[A, B, C]

  def filter[A](taskID: String, taskVersion: Int)(
      fun: A => Boolean): TaskDefinition[EColl[A], EColl[A]] =
    macro Macros
      .filterMacro[A]

  def mapConcat[A, B](taskID: String, taskVersion: Int)(
      fun: A => Iterable[B]): TaskDefinition[EColl[A], EColl[B]] =
    macro Macros
      .mapConcatMacro[A, B]

  def sortBy[A](taskID: String, taskVersion: Int)(
      batchSize: Int,
      fun: A => String): TaskDefinition[EColl[A], EColl[A]] =
    macro Macros
      .sortByMacro[A]

  def groupBy[A](taskID: String, taskVersion: Int)(
      parallelism: Int,
      fun: A => String): TaskDefinition[EColl[A], EColl[Seq[A]]] =
    macro Macros
      .groupByMacro[A]

  def outerJoinBy[A](taskID: String, taskVersion: Int)(
      parallelism: Int,
      fun: A => String): TaskDefinition[List[EColl[A]], EColl[Seq[Option[A]]]] =
    macro Macros.outerJoinByMacro[A]

  def innerJoinBy2[A, B](taskID: String, taskVersion: Int)(
      parallelism: Int,
      funA: A => String,
      funB: B => String): TaskDefinition[(EColl[A], EColl[B]), EColl[(A, B)]] =
    macro Macros.innerJoinBy2Macro[A, B]

  def outerJoinBy2[A, B](taskID: String, taskVersion: Int)(
      parallelism: Int,
      funA: A => String,
      funB: B => String): TaskDefinition[(EColl[A], EColl[B]),
                                         EColl[(Option[A], Option[B])]] =
    macro Macros.outerJoinBy2Macro[A, B]

  def foldLeft[A, B](taskID: String, taskVersion: Int)(
      zero: B,
      fun: (B, A) => B): TaskDefinition[EColl[A], B] =
    macro Macros
      .foldLeftMacro[A, B]

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
// circe:
// case class EColl[T](sf: SharedFile) {
//   def source(implicit decoder: Decoder[T],
//              tsc: TaskSystemComponents): Source[T, _] =
//     sf.source
//       .via(
//           Framing.delimiter(ByteString("\n"),
//                             maximumFrameLength = Int.MaxValue,
//                             allowTruncation = false))
//       .map(
//           line =>
//             io.circe.parser
//               .parse(line.utf8String)
//               .right
//               .flatMap(decoder.decodeJson)
//               .right
//               .get)
//
//   def map[A](f: T => A)(
//       implicit decoderT: Decoder[T],
//       encoderA: Encoder[A]): TaskDefinition[EColl[T], EColl[A]] =
//     AsyncTask[EColl[T], EColl[A]]("name1", 1) { t => implicit ctx =>
//       EColl.fromSource[A](t.source.map(f), "filenam e")
//     }
// }
//
// object EColl {
//   def fromSource[T](source: Source[T, _], name: String)(
//       implicit encoder: Encoder[T],
//       tsc: TaskSystemComponents): Future[EColl[T]] = {
//     val s2 = source.map(t => ByteString(encoder.apply(t).noSpaces + "\n"))
//     SharedFile(s2, name).map(sf => EColl[T](sf))(tsc.executionContext)
//   }
// }
