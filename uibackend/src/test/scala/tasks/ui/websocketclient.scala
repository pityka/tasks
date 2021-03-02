package tasks.ui

import akka.actor._
import akka.stream.scaladsl._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._
import scala.concurrent._
import scala.concurrent.duration._

object WebSocketClient {
  def make(url: String)(foreach: String => Unit) = {
    implicit val AS = ActorSystem()
    import AS.dispatcher

    val sink: Sink[Message, Future[_]] =
      Sink.foreach {
        case message: TextMessage.Strict =>
          foreach(message.text)
        case other => println(other)
      }

    val input =
      Source
        .tick(500 milliseconds, 500 milliseconds, None)
        .map(_ => TextMessage("boo"))

    val flow =
      Flow.fromSinkAndSourceMat(sink, input)(Keep.left)

    val (upgradeResponse, closed) =
      Http().singleWebSocketRequest(WebSocketRequest(url), flow)

    val connected = upgradeResponse.map { upgrade =>
      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        akka.Done
      } else {
        throw new RuntimeException(
          s"Connection failed: ${upgrade.response.status}"
        )
      }
    }

    connected.onComplete(println)
    closed.foreach(_ => println("closed"))
  }
}
