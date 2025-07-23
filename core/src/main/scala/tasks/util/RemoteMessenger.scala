package tasks.util
import org.http4s.{Message => _, _}
import org.http4s.dsl.io._
import cats.effect.IO
import cats.effect.kernel.Ref
import fs2.concurrent.Channel
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import org.http4s.client.Client
import org.http4s.ember.server.EmberServerBuilder
import cats.effect.kernel.Resource
import org.http4s.ember.client.EmberClientBuilder
import tasks.util.config.TasksConfig
import tasks.deploy.HostConfiguration
import tasks.deploy.LocalConfiguration
import tasks.fileservice.FileServicePrefix

import cats.instances.list
import tasks.util.message._
private[tasks] class RemoteMessenger(
    client: Client[IO],
    listeningUri: org.http4s.Uri,
    peerUri: org.http4s.Uri,
    localMessenger: LocalMessenger
) extends Messenger {

  def listeningAddress = Some(listeningUri.toString)
  def submit(message: Message): IO[Unit] =
    localMessenger.channels.get.flatMap { channels =>
      channels.get(message.to.withoutUri) match {
        case None =>
          val messageWithRemoteUri =RemoteMessenger.addUri(message, listeningUri)
          scribe.debug(s"Submitting message to remote messenger ${message.data.getClass()}. Local channel not found with channel message key ${message.to.withoutUri}. Submit message ${messageWithRemoteUri.to} to peer $peerUri")
          RemoteMessenger
            .submit0(
              message = messageWithRemoteUri,
              client = client,
              peerUri = peerUri
            )
            .void
        case _ => localMessenger.submit(message)
      }
    }
  def subscribe(address: Address): IO[fs2.Stream[IO, Message]] =
    IO(scribe.trace(s"Subscribing to $address")) *> localMessenger.subscribe(
      address
    )
}

private[tasks] object RemoteMessenger {

  def addUri(message: Message, listeningUri: org.http4s.Uri) = {
    message.copy(from =
      message.from.copy(listeningUri = Some(listeningUri.toString))
    )

  }

  import org.http4s.headers.`Content-Type`

  implicit private val entityEncoder: EntityEncoder[IO, Message] =
    EntityEncoder.encodeBy[IO, Message](
      Headers(`Content-Type`(MediaType.application.json))
    ) {
      EntityEncoder
        .byteArrayEncoder[IO]
        .contramap[Message](
          com.github.plokhotnyuk.jsoniter_scala.core.writeToArray(_)
        )
        .toEntity(_)
    }
  implicit private val entityDecoder: EntityDecoder[IO, Message] = {
    def make(bytes: Array[Byte]): IO[Either[DecodeFailure, Message]] =
      IO(
        com.github.plokhotnyuk.jsoniter_scala.core.readFromArray[Message](bytes)
      ).attempt.map(either =>
        either.left.map(throwable =>
          MalformedMessageBodyFailure("JSON decoding failed", Option(throwable))
        )
      )

    EntityDecoder.decodeBy(MediaType.application.json) {
      EntityDecoder
        .byteArrayDecoder[IO]
        .flatMapR { bytes =>
          DecodeResult(make(bytes))
        }
        .decode(_, strict = true)
    }
  }

  private def route(localMessenger: LocalMessenger, prefix: String) =
    HttpRoutes.of[IO] { case request @ POST -> Root / prefix =>
      request.decode[Message] { message =>
        IO(
          scribe.trace(
            s"HTTP server received message ${message.from} ${message.to} ${message.data.getClass}"
          )
        ) *>
          localMessenger
            .submit(message)
            .flatMap(_ =>
              Ok(
                s"submitted ${message.from} ${message.to} ${message.data.getClass()}"
              )
            )
      }
    }
  private def submit0(
      message: Message,
      client: Client[IO],
      peerUri: org.http4s.Uri
  ) = {
    val num = scala.util.Random.nextLong()
    val effectiveUri = message.to.listeningUri
          .map(s =>
            org.http4s.Uri
              .fromString(s)
              .toOption
              .getOrElse(throw new RuntimeException(s"Can't parse $s"))
          )
          .getOrElse(peerUri)
    val request =
      Request[IO](
        method = Method.POST,
        uri = effectiveUri
      ).withEntity(
        message
      )
    
    IO(
      scribe.trace(
        s"Submit via http $num $effectiveUri (uri in message ${message.to.listeningUri}) ${message.from} ${message.to} ${message.data.getClass()}"
      )
    ) *>
      client.expect[String](request).attempt.flatMap {
        case Right(result) =>
          IO(scribe.trace(s"Http response $num: $result")).map(_ => result)
        case Left(e) =>
          IO(
            scribe.error(
              s"Http request failed $num. $request. ${e.getMessage} ${message.from} ${message.to} ${message.data.getClass()}"
            )
          ) *> IO.pure("")
      }
  }

  /** Will receive http POST messages on http://$bindHost:$bindPort/$bindPrefix
    *
    * @param bindHost
    *   The host name or IP to which this service will bind
    * @param bindPort
    *   The port to which this service will bind
    * @param bindPrefix
    *   An http path prefix
    * @param peerUri
    *   The http uri to which this messenger will submit
    * @return
    */
  def make(
      bindHost: String,
      bindPort: Int,
      bindPrefix: String,
      peerUri: org.http4s.Uri
  ) = {
    import com.comcast.ip4s._

    LocalMessenger.make.flatMap { localMessenger =>
      val r = route(localMessenger, bindPrefix)
      val listeningUri = {
        val u = s"http://$bindHost:$bindPort/$bindPrefix"
        org.http4s.Uri
          .fromString(u)
          .toOption
          .getOrElse(throw new RuntimeException(s"Can't parse $u"))
      }
      val server = EmberServerBuilder
        .default[IO]
        .withHost(
          com.comcast.ip4s.Host
            .fromString(bindHost)
            .getOrElse(throw new RuntimeException(s"Can't parse $bindHost"))
        )
        .withPort(
          com.comcast.ip4s.Port.fromInt(bindPort).get
        )
        .withHttpApp(r.orNotFound)
        .withShutdownTimeout(scala.concurrent.duration.Duration.Zero)
        .build
      val client = EmberClientBuilder
        .default[IO]
        .build
      client.flatMap { client =>
        server.map { server =>
          scribe.info(
            s"Remote messenger with ${listeningUri} built. Peer: $peerUri"
          )
          new RemoteMessenger(
            client = client,
            peerUri = peerUri,
            listeningUri = listeningUri,
            localMessenger = localMessenger
          )
        }
      }
    }

  }
}
