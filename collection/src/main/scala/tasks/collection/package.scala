package tasks.collection

import flatjoin._
// import io.circe._
// import io.circe.syntax._
// import io.circe.parser._
import upickle.default.{Reader, Writer}
import tasks._
import java.io.File
import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import scala.concurrent.Future

case class EColl[T](partitions: List[SharedFile])
    extends ResultWithSharedFiles(partitions: _*) {

  def basename: String =
    partitions.head.name.split("\\.").dropRight(1).mkString(".")

  def ++(that: EColl[T]): EColl[T] = EColl[T](partitions ++ that.partitions)

  def source(implicit decoder: Reader[T],
             tsc: TaskSystemComponents): Source[T, _] =
    Source(partitions).flatMapConcat(
        _.source.via(Compression.gunzip())
          .via(Framing.delimiter(ByteString("\n"),
                                 maximumFrameLength = Int.MaxValue,
                                 allowTruncation = false))
          .map(line => decoder.read(upickle.json.read(line.utf8String))))

  def source(i: Int)(implicit decoder: Reader[T],
                     tsc: TaskSystemComponents): Source[T, _] =
    partitions(i).source.via(Compression.gunzip())
      .via(Framing.delimiter(ByteString("\n"),
                             maximumFrameLength = Int.MaxValue,
                             allowTruncation = false))
      .map(line => decoder.read(upickle.json.read(line.utf8String)))

}

object EColl {

  def partitionsFromSource[T:  StringKey](source: Source[T, _],
                                                 name: String)(
      implicit encoder: Writer[T],
      tsc: TaskSystemComponents
  ) = {
    implicit val ec = tsc.executionContext
    implicit val mat = tsc.actorMaterializer

    def sinkToFlow[T, U](sink: Sink[T, U]): Flow[T, U, U] =
    Flow.fromGraph(GraphDSL.create(sink) { implicit builder => sink =>
      FlowShape.of(sink.in, builder.materializedValue)
    })


    val shardFlow: Flow[T, Seq[File], NotUsed] = {

    val files = 0 until 128 map { i =>
      val file = File.createTempFile("shard_" + i + "_", "shard")
      file.deleteOnExit
      (i, file)
    } toMap

    val hash = (t: T) =>
      math.abs(
        scala.util.hashing.MurmurHash3
          .stringHash(implicitly[StringKey[T]].key(t)) % 128)

    val flows: List[Flow[(ByteString, Int), Seq[File], _]] = files.map {
      case (idx, file) =>
        sinkToFlow(
          Flow[(ByteString, Int)]
            .filter(_._2 == idx) //todo
            .map(_._1++ByteString("\n")).via(Compression.gzip)
            .toMat(FileIO.toPath(file.toPath))(Keep.right)
            .mapMaterializedValue(_.map(_ => file :: Nil))).mapAsync(1)(x => x)

    }.toList

    val shardFlow: Flow[(ByteString, Int), Seq[File], NotUsed] = Flow
      .fromGraph(
        GraphDSL.create() { implicit b =>
          import GraphDSL.Implicits._
          // Use Partition instead
          val broadcast = b.add(Broadcast[(ByteString, Int)](flows.size))
          val merge = b.add(Merge[Seq[File]](flows.size))
          flows.zipWithIndex.foreach {
            case (f, i) =>
              val flowShape = b.add(f)
              broadcast.out(i) ~> flowShape.in
              flowShape.out ~> merge.in(i)
          }

          new FlowShape(broadcast.in, merge.out)
        }
      )
      .reduce(_ ++ _)

    Flow[T].map {
      case elem =>
        ByteString(upickle.json.write(encoder.write(elem))) -> hash(elem)
    }.viaMat(shardFlow)(Keep.right)
  }

    source
      .via(shardFlow)
      .mapAsync(1) { files =>
        Source(files.zipWithIndex.toList)
          .mapAsync(1)(file =>
                SharedFile(file._1, name = name + "." + file._2))
          .runWith(Sink.seq)
      }
      .map { sfs =>
        EColl[T](sfs.toList)
      }
      .runWith(Sink.head)
  }

  def fromSource[T](source: Source[T, _], name: String)(
      implicit encoder: Writer[T],
      tsc: TaskSystemComponents): Future[EColl[T]] = {
    val s2 =
      source.map(t => ByteString(upickle.json.write(encoder.write(t)) + "\n")).via(Compression.gzip)

    SharedFile(s2, name + ".0")
      .map(sf => EColl[T](sf :: Nil))(tsc.executionContext)
  }

  import scala.language.experimental.macros

  def map[A, B](taskID: String, taskVersion: Int)(
      fun: A => B): TaskDefinition[EColl[A], EColl[B]] = macro Macros
    .mapMacro[A, B]

  def filter[A](taskID: String, taskVersion: Int)(
      fun: A => Boolean): TaskDefinition[EColl[A], EColl[A]] = macro Macros
    .filterMacro[A]

  def mapConcat[A, B](taskID: String, taskVersion: Int)(
      fun: A => Iterable[B]): TaskDefinition[EColl[A], EColl[B]] = macro Macros
    .mapConcatMacro[A, B]

  def sortBy[A](taskID: String, taskVersion: Int)(
      batchSize: Int,
      fun: A => String): TaskDefinition[EColl[A], EColl[A]] = macro Macros
    .sortByMacro[A]

  def groupBy[A](taskID: String, taskVersion: Int)(
      parallelism: Int,
      fun: A => String): TaskDefinition[EColl[A], EColl[Seq[A]]] = macro Macros
    .groupByMacro[A]

  def outerJoinBy[A](
      taskID: String,
      taskVersion: Int)(parallelism: Int, fun: A => String): TaskDefinition[
      List[EColl[A]],
      EColl[Seq[Option[A]]]] = macro Macros.outerJoinByMacro[A]

  def foldLeft[A, B](taskID: String, taskVersion: Int)(
      zero: B,
      fun: (B, A) => B): TaskDefinition[EColl[A], B] = macro Macros
    .foldLeftMacro[A, B]

  def reduce[A](taskID: String, taskVersion: Int)(
      fun: (A, A) => A): TaskDefinition[EColl[A], A] = macro Macros
    .reduceMacro[A]

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
