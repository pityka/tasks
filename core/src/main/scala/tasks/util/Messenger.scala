package tasks.util

import org.http4s._
import org.http4s.dsl.io._
import cats.effect.IO
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import cats.effect.kernel.Resource
import tasks.util.config.TasksConfig
import tasks.deploy.HostConfiguration
import tasks.deploy.LocalConfiguration

import tasks.wire._

case class Address(value: String, listeningUri: Option[String]) {
  private[util] def withoutUri = Address(value, None)
  def withAddress(s: Option[String]) = copy(listeningUri = listeningUri.orElse(s))
  override def equals(that: Any): Boolean = {
    that match {
      case Address(v,_) => value == v
      case _ => false
    }
  }

  override def hashCode(): Int =value.hashCode()
}
object Address {
  def apply(value: String) : Address = Address(value, None)
}

case class Message(data: MessageData, from: Address, to: Address)
object Message {
  implicit val codec: JsonValueCodec[Message] =
    com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker.make
}

trait Messenger {
  def listeningAddress : Option[String]
  def submit(message: Message): IO[Unit]
  def subscribe(address: Address): IO[fs2.Stream[IO, Message]]

}

object Messenger {
  def make(hostConfig: HostConfiguration): Resource[IO, Messenger] = {
    hostConfig match {
      case _: LocalConfiguration => LocalMessenger.make
      case _ =>
        val externalAddress =
          hostConfig.myAddressExternal.getOrElse(hostConfig.myAddressBind)
        val internalAddress = hostConfig.myAddressBind

        
        val peerUri = {
          val str =
          s"http://${hostConfig.master.hostName}:${hostConfig.master.port}"
          org.http4s.Uri
          .fromString(str)
          .getOrElse(throw new RuntimeException(s"Can't parse $str"))
        }
        RemoteMessenger.make(
          bindHost = internalAddress.hostName,
          bindPort = internalAddress.port,
          peerUri
        )
    }

  }
}
