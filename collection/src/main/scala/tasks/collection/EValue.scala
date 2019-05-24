package tasks.collection

import tasks.queue._
import tasks._
import akka.stream.scaladsl._
import akka.util.ByteString
import scala.concurrent.Future
import com.typesafe.scalalogging.StrictLogging

case class EValue[T](data: SharedFile) extends WithSharedFiles() {
  def basename: String =
    data.name

  def get(implicit decoder: Deserializer[T],
          tsc: TaskSystemComponents): Future[T] =
    data.source
      .runFold(ByteString())(_ ++ _)(tsc.actorMaterializer)
      .map { bs =>
        val decoded = decoder(bs.toArray)
        decoded.left.foreach { error =>
          EValue.log.error(s"Can't deserialize $data. $error")
        }
        decoded.right.get
      }(tsc.actorMaterializer.executionContext)

  def source(implicit decoder: Deserializer[T],
             tsc: TaskSystemComponents): Source[T, _] =
    Source.lazilyAsync(() => this.get)

}

object EValue extends StrictLogging {

  def log = logger

  def getByName[T](name: String)(
      implicit
      tsc: TaskSystemComponents): Future[Option[EValue[T]]] =
    SharedFile
      .getByName(name)
      .map(sf => sf.map(EValue.apply[T]))(
        tsc.actorMaterializer.executionContext)

  def apply[T](t: T, name: String)(
      implicit encoder: Serializer[T],
      tsc: TaskSystemComponents): Future[EValue[T]] =
    SharedFile
      .apply(Source.single(ByteString(encoder(t))), name: String)
      .map(EValue[T](_))(tsc.actorMaterializer.executionContext)
}
