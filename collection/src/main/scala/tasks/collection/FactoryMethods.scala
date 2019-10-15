package tasks.ecoll

import tasks.queue._
import tasks._
import akka.stream.scaladsl._
import akka.util.ByteString
import scala.concurrent.Future
import com.typesafe.scalalogging.StrictLogging

trait FactoryMethods extends StrictLogging { self: Constants =>

  def concatenate[T](ecolls: Seq[EColl[T]], name: Option[String] = None)(
      implicit tsc: TaskSystemComponents
  ): Future[EColl[T]] = {
    implicit val mat = tsc.actorMaterializer
    implicit val ec = tsc.executionContext
    val dataName = name.getOrElse(java.util.UUID.randomUUID.toString)
    val indexName = dataName + ".bidx"
    val source = ecolls.map(_.data.source).reduce(_ ++ _)
    val dataFile = SharedFile(source, name = dataName)
    val concatenatedIndex = for {
      seq <- Future.traverse(ecolls) { ecoll =>
        val dataLength = ecoll.data.byteSize
        val index = ecoll.indexData.source.runFold(ByteString.empty)(_ ++ _)
        index.map(i => (dataLength, i))
      }
      concatenated = lame.index.Index.concatenate(seq.iterator)
      file <- SharedFile(Source.single(concatenated), indexName)
    } yield file

    for {
      dataFile <- dataFile
      concatenatedIndex <- concatenatedIndex
    } yield EColl(dataFile, concatenatedIndex, name)
  }

  def fromSource[T](
      source: Source[T, _],
      name: Option[String] = None,
      parallelism: Int = 1
  )(
      implicit encoder: Serializer[T],
      tsc: TaskSystemComponents
  ): Future[EColl[T]] = {
    implicit val mat = tsc.actorMaterializer
    source.runWith(sink[T](name, parallelism))
  }

  def sink[T](name: Option[String] = None, parallelism: Int = 1)(
      implicit encoder: Serializer[T],
      tsc: TaskSystemComponents
  ): Sink[T, Future[EColl[T]]] = {
    implicit val mat = tsc.actorMaterializer
    implicit val ec = mat.executionContext

    val gzipPar = 1 //if (parallelism == 1) 1 else parallelism / 2
    val encoderPar = if (parallelism == 1) 1 else parallelism - gzipPar

    val encoderFlow =
      lame.Parallel
        .mapAsync[T, ByteString](encoderPar, ElemBufferSize)(
          elem => ByteString(encoder.apply(elem)) ++ Eol
        )

    val dataFileName = name.getOrElse(java.util.UUID.randomUUID.toString)
    val indexFileName = dataFileName + ".bidx"

    val dataSink = SharedFile
      .sink(dataFileName)
      .getOrElse(
        throw new RuntimeException(
          "No sink created because sinks without a centralized storage are not implemented. Use a centralized storage to support this."
        )
      )
    val indexSink = SharedFile
      .sink(indexFileName)
      .getOrElse(
        throw new RuntimeException(
          "No sink created because sinks without a centralized storage are not implemented. Use a centralized storage to support this."
        )
      )

    val bgzipSink = lame.BlockGzip.sinkWithIndex(
      dataSink,
      indexSink,
      compressionLevel = 1,
      customDeflater = None
    )

    encoderFlow.toMat(bgzipSink)(Keep.right).mapMaterializedValue {
      case (dataSF, indexSF) =>
        for {
          dataSF <- dataSF
          indexSF <- indexSF
        } yield EColl(dataSF, indexSF, name)

    }

  }

  def single[T](t: T, name: Option[String] = None)(
      implicit encoder: Serializer[T],
      tsc: TaskSystemComponents
  ): Future[EColl[T]] =
    fromSource(Source.single(t), name)

  def fromIterator[T: Serializer](
      it: Iterator[T],
      name: Option[String] = None,
      parallelism: Int = 1
  )(implicit tsc: TaskSystemComponents) = {
    val source = Source.fromIterator(() => it)
    fromSource(source, name, parallelism)
  }

}
