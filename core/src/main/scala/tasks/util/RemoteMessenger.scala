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
    val listeningUri: org.http4s.Uri,
    val peerUri: org.http4s.Uri,
    localMessenger: LocalMessenger
) extends Messenger {

  def listeningAddress = Some(listeningUri.toString)
  def submit(message: Message): IO[Unit] =
    localMessenger.channels.get.flatMap { channels =>
      channels.get(message.to.withoutUri) match {
        case None =>
          val messageWithRemoteUri =
            RemoteMessenger.addUri(message, listeningUri)
          scribe.debug(
            "Submit",
            message,
            this,
            scribe.data(
              "explain",
              "Can't deliver message to a local subscription because no open channels found, thus submit message to remote messenger."
            )
          )
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
    IO(
      scribe.trace(
        s"Subscribe",
        address,
        this,
        scribe.data(
          "explain",
          "Returns a message stream to the caller on this address."
        )
      )
    ) *> localMessenger
      .subscribe(
        address
      )
}

private[tasks] object RemoteMessenger {

  implicit def toLogFeature(rm: RemoteMessenger): scribe.LogFeature =
    scribe.data(
      Map(
        "remote-messenger-peer-uri" -> rm.peerUri,
        "remote-messenger-listening-uri" -> rm.listeningUri,
        "remote-messenger-explain" -> "peer uri is inserted into outgoing messages so that replies can be made to that uri. listening uri is the address on which the http server is bound."
      )
    )

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
            s"HTTP receive",
            message
          )
        ) *>
          localMessenger
            .submit(message)
            .flatMap(_ =>
              Ok(
                s"submitted message"
              )
            )
      }
    }
  private def submit0(
      message: Message,
      client: Client[IO],
      peerUri: org.http4s.Uri
  ) = {
    val num = CorrelationId.make
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

    val requestData = scribe.data(
      Map(
        "uri" -> effectiveUri
      )
    )

    IO(
      scribe.trace(s"HTTP request", message, num, requestData)
    ) *>
      client.expect[String](request).attempt.flatMap {
        case Right(result) =>
          IO(
            scribe.trace(
              s"HTTP response",
              num,
              requestData,
              message,
              scribe.data("response-body", result)
            )
          )
            .map(_ => result)
        case Left(e) =>
          IO(
            scribe.error(
              s"HTTP request failed.",
              num,
              requestData,
              message,
              e
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
          val rm = new RemoteMessenger(
            client = client,
            peerUri = peerUri,
            listeningUri = listeningUri,
            localMessenger = localMessenger
          )
          scribe.info(
            s"Remote messenger built.",
            rm
          )
          rm
        }
      }
    }

  }
}
