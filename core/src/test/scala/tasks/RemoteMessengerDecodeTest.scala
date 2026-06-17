package tasks

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global

import org.http4s.{MediaType, Method, Request, Status, Uri}
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.headers.`Content-Type`

import tasks.util.RemoteMessenger

import scala.concurrent.duration._

class RemoteMessengerDecodeTest extends AnyFunSuite with Matchers {

  private def freePort(): Int = {
    val s = new java.net.ServerSocket(0)
    try s.getLocalPort
    finally s.close()
  }

  private val dummyPeerUri =
    Uri.fromString("http://127.0.0.1:1/").toOption.get

  private def httpClient: Resource[IO, Client[IO]] =
    EmberClientBuilder.default[IO].build

  private def postJson(
      c: Client[IO],
      url: String,
      body: String
  ): IO[(Status, String)] = {
    val req = Request[IO](
      method = Method.POST,
      uri = Uri.fromString(url).toOption.get
    ).withEntity(body)
      .putHeaders(`Content-Type`(MediaType.application.json))
    c.run(req).use(r => r.bodyText.compile.string.map(b => (r.status, b)))
  }

  private def serverAndClient(port: Int): Resource[IO, Client[IO]] =
    for {
      _ <- RemoteMessenger.make(
        bindHost = "127.0.0.1",
        bindPort = port,
        bindPrefix = "msg",
        peerUri = dummyPeerUri
      )
      c <- httpClient
    } yield c

  test("POST with malformed JSON returns 400 with decode failure description") {
    val port = freePort()
    val (status, body) = serverAndClient(port)
      .use(c => postJson(c, s"http://127.0.0.1:$port/msg", "{not valid json"))
      .timeout(10.seconds)
      .unsafeRunSync()

    status shouldBe Status.BadRequest
    body should include("failed to decode Message")
  }

  test(
    "POST with JSON that does not match Message schema returns 400 with cause"
  ) {
    val port = freePort()
    val (status, body) = serverAndClient(port)
      .use(c => postJson(c, s"http://127.0.0.1:$port/msg", "{}"))
      .timeout(10.seconds)
      .unsafeRunSync()

    status shouldBe Status.BadRequest
    body should include("failed to decode Message")
    body should include("cause:")
  }
}
