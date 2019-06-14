package tasks.ecoll

import tasks.queue._
import tasks.ecoll.ops._
import tasks._
import tasks.util.rightOrThrow
import akka.stream.scaladsl._
import akka.util.ByteString
import akka.NotUsed
import scala.concurrent.Future
import lame.index.Index

// open ended
case class Range(fromIdx: Long, toIdx: Long) {
  def size = toIdx - fromIdx
  def nonEmpty = size > 0
  override def toString = s"$fromIdx-$toIdx"
}

object Range {
  import io.circe._
  import io.circe.generic.semiauto._
  implicit val encoder: Encoder[Range] = deriveEncoder[Range]
  implicit val decoder: Decoder[Range] = deriveDecoder[Range]
}

case class EColl[T](
    data: SharedFile,
    indexData: SharedFile,
    name: Option[String]
) {

  def withName(name: String) = copy(name = Some(name))

  def loadIndex(implicit tsc: TaskSystemComponents) = {
    implicit val mat = tsc.actorMaterializer
    implicit val ec = tsc.executionContext
    for {
      indexBytes <- indexData.source.runFold(ByteString.empty)(_ ++ _)
    } yield {
      Index(indexBytes)
    }
  }

  def ranges(numberOfRanges: Int)(implicit tsc: TaskSystemComponents) = {
    implicit val ec = tsc.executionContext
    loadIndex.map { index =>
      val totalLength = index.length
      val rangeLength = math.max(1, (totalLength / numberOfRanges) + 1)
      (0 until numberOfRanges)
        .map { i =>
          val start = i * rangeLength
          val end = math.min((i + 1) * rangeLength, totalLength)
          Range(start, end)
        }
        .filter(_.nonEmpty)
    }
  }

  def sourceOfRange(range: Range,
                    parallelismOfDeserialization: Int = 1,
                    parsedIndex: Option[lame.index.Index] = None)(
      implicit decoder: Deserializer[T],
      tsc: TaskSystemComponents
  ) =
    sourceFrom(range.fromIdx, parallelismOfDeserialization, parsedIndex)
      .take(range.size)

  def sourceFrom(
      fromIndex: Long = 0L,
      parallelismOfDeserialization: Int = 1,
      parsedIndex: Option[lame.index.Index] = None
  )(
      implicit decoder: Deserializer[T],
      tsc: TaskSystemComponents
  ): Source[T, NotUsed] = {

    val decodeElements =
      lame.Parallel
        .mapAsync[ByteString, T](
          parallelism = parallelismOfDeserialization,
          bufferSize = EColl.ElemBufferSize
        )(line => rightOrThrow(decoder(line.toArray)))(
          tsc.actorMaterializer.executionContext
        )

    implicit val ec = tsc.actorMaterializer.executionContext

    val indexQueryResult =
      if (fromIndex <= 0L) Future.successful(Some(Index.QueryResult(0L, 0L)))
      else {
        parsedIndex match {
          case Some(index) => Future.successful(index.query(fromIndex))
          case None        => loadIndex.map(_.query(fromIndex))
        }
      }

    val futureSource = indexQueryResult.map {
      case Some(Index.QueryResult(vfp, skip)) =>
        lame.BlockGunzip
          .sourceFromFactory(virtualFilePointer = vfp, customInflater = None) {
            fileOffset =>
              data.source(fromOffset = fileOffset)
          }
          .via(EColl.decodeFrame)
          .filter(_.nonEmpty)
          .drop(skip)
          .via(decodeElements)
      case None => Source.empty[T]
    }

    Source
      .fromFutureSource(futureSource)
      .mapMaterializedValue(_ => NotUsed)

  }

  // provide a specialized source for selective column retrieval

  def toSeq(parallelismOfDeserialization: Int)(
      implicit decoder: Deserializer[T],
      tsc: TaskSystemComponents
  ): Future[Seq[T]] = {
    implicit val mat = tsc.actorMaterializer
    sourceFrom(parallelismOfDeserialization = parallelismOfDeserialization)
      .runWith(Sink.seq)
  }

  def take(n: Long, parallelismOfDeserialization: Int)(
      implicit decoder: Deserializer[T],
      tsc: TaskSystemComponents
  ): Future[Seq[T]] = {
    implicit val mat = tsc.actorMaterializer
    sourceFrom(parallelismOfDeserialization = parallelismOfDeserialization)
      .take(n)
      .runWith(Sink.seq)
  }

  def head(
      implicit decoder: Deserializer[T],
      tsc: TaskSystemComponents
  ): Future[Option[T]] = {
    implicit val mat = tsc.actorMaterializer
    sourceFrom().runWith(Sink.headOption)
  }

  def delete(implicit tsc: TaskSystemComponents) = {
    implicit val ec = tsc.executionContext
    for {
      _ <- data.delete
      _ <- indexData.delete
    } yield ()
  }

}

trait ECollSerializers {
  import io.circe._
  import io.circe.generic.semiauto._
  implicit def encoder[A]: Encoder[EColl[A]] = deriveEncoder[EColl[A]]
  implicit def decoder[A]: Decoder[EColl[A]] = deriveDecoder[EColl[A]]
}

object EColl
    extends SimpleMapOps
    with CollectOps
    with FilterOps
    with GenericMapOps
    with ScanOps
    with MapConcatOps
    with SortOps
    with GroupByOps
    with Group2Ops
    with JoinOps
    with Join2Ops
    with TakeOps
    with FoldOps
    with GroupedOps
    with ReduceOps
    with DistinctOps
    with FactoryMethods
    with FlatjoinSupport
    with Constants
    with Framing
    with ECollSerializers
