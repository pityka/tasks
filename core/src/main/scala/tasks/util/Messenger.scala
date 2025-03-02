package tasks.util

import org.http4s.{Message => _, _}
import org.http4s.dsl.io._
import cats.effect.IO
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import cats.effect.kernel.Resource
import tasks.util.config.TasksConfig
import tasks.deploy.HostConfiguration
import tasks.deploy.LocalConfiguration
import tasks.util.message._
import tasks.wire._

trait Messenger {
  def listeningAddress: Option[String]
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
