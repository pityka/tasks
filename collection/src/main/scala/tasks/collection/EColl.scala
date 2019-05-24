package tasks.collection

import tasks.queue._
import tasks._
import tasks.util.AkkaStreamComponents
import akka.stream.scaladsl._
import akka.util.ByteString

case class EColl[T](partitions: List[SharedFile], length: Long)
    extends WithSharedFiles(partitions) {

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
          List(decoder(line.toArray).right.get))(
          tsc.actorMaterializer.executionContext)

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
      .map(line => decoder.apply(line.toArray).right.get)

}

object EColl
    extends MacroOps
    with FactoryMethods
    with FlatjoinSupport
    with Constants
    with Framing
