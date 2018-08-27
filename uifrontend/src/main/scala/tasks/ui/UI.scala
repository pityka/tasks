package tasks.ui

import org.scalajs.dom.raw._
import org.scalajs.dom
import scala.scalajs.js.annotation._
import scalatags.JsDom.all._
import akka.ui._
import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import scala.concurrent.ExecutionContext.Implicits.global

object WebSocketHelper {
  def open(address: String)(
      implicit
      AM: ActorMaterializer): (Source[MessageEvent, _], Sink[String, _]) = {
    val ws = new dom.WebSocket(address)
    val source = ws.source(_.onmessage_=).watchTermination() {
      (_, terminationFuture) =>
        terminationFuture.foreach(_ => ws.close())
    }
    val sink = Sink.foreach[String](msg => ws.send(msg))
    (source, sink)
  }
}

class UI(container: Node) {

  implicit val system = ActorSystem("AkkaUI")
  implicit val materializer = ActorMaterializer()

  val dataContainer = span().render

  val dataContainerSink =
    Flow[UIState].map(_.toString).to(dataContainer.sink(_.textContent_=))

  val root = div(
    code(dataContainer)
  )

  val (wsSource, wsSink) = WebSocketHelper.open("ws://localhost:28880/states")

  val keepAliveTicks = {
    import scala.concurrent.duration._
    Source.tick(0 seconds, 500 milliseconds, "tick")
  }

  keepAliveTicks.runWith(wsSink)

  wsSource
    .map(wsMessage =>
      io.circe.parser.decode[UIState](wsMessage.data.toString).right.get)
    .runWith(dataContainerSink)

  container.appendChild(root.render)

}

@JSExportTopLevel("tasks.ui.Main")
object Main {

  @JSExport
  def bind(parent: Node) = {
    new UI(parent)
  }

}
