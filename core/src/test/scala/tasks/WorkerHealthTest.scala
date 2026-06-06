package tasks

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import cats.effect.unsafe.implicits.global

import org.http4s.{Method, Request, Status, Uri}
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

import tasks.util.RemoteMessenger

import scala.concurrent.duration._

class WorkerHealthTest extends AnyFunSuite with Matchers {

  private def freePort(): Int = {
    val s = new java.net.ServerSocket(0)
    try s.getLocalPort
    finally s.close()
  }

  private val dummyPeerUri =
    Uri.fromString("http://127.0.0.1:1/").toOption.get

  private def client: Resource[IO, Client[IO]] =
    EmberClientBuilder.default[IO].build

  private def getStatus(c: Client[IO], url: String): IO[Status] =
    c.run(
      Request[IO](
        method = Method.GET,
        uri = Uri.fromString(url).toOption.get
      )
    ).use(resp => IO.pure(resp.status))

  test("GET /health returns 200 when worker is healthy") {
    val port = freePort()
    val program = for {
      healthRef <- Resource.eval(Ref.of[IO, Boolean](true))
      _ <- RemoteMessenger.make(
        bindHost = "127.0.0.1",
        bindPort = port,
        bindPrefix = "msg",
        peerUri = dummyPeerUri,
        workerHealth = healthRef.get
      )
      c <- client
    } yield c

    val status = program
      .use(c => getStatus(c, s"http://127.0.0.1:$port/health"))
      .timeout(10.seconds)
      .unsafeRunSync()

    status shouldBe Status.Ok
  }

  test("GET /health returns 503 when worker cannot self-shutdown") {
    val port = freePort()
    val program = for {
      healthRef <- Resource.eval(Ref.of[IO, Boolean](false))
      _ <- RemoteMessenger.make(
        bindHost = "127.0.0.1",
        bindPort = port,
        bindPrefix = "msg",
        peerUri = dummyPeerUri,
        workerHealth = healthRef.get
      )
      c <- client
    } yield c

    val status = program
      .use(c => getStatus(c, s"http://127.0.0.1:$port/health"))
      .timeout(10.seconds)
      .unsafeRunSync()

    status shouldBe Status.ServiceUnavailable
  }

  test("GET /health reflects the predicate on every request (flips on shutdown)") {
    val port = freePort()
    val program: Resource[IO, (Client[IO], Ref[IO, Boolean])] = for {
      healthRef <- Resource.eval(Ref.of[IO, Boolean](true))
      _ <- RemoteMessenger.make(
        bindHost = "127.0.0.1",
        bindPort = port,
        bindPrefix = "msg",
        peerUri = dummyPeerUri,
        workerHealth = healthRef.get
      )
      c <- client
    } yield (c, healthRef)

    val (before, after) = program
      .use { case (c, ref) =>
        for {
          a <- getStatus(c, s"http://127.0.0.1:$port/health")
          _ <- ref.set(false)
          b <- getStatus(c, s"http://127.0.0.1:$port/health")
        } yield (a, b)
      }
      .timeout(10.seconds)
      .unsafeRunSync()

    before shouldBe Status.Ok
    after shouldBe Status.ServiceUnavailable
  }
}
