/*
 * The MIT License
 *
 * Modified work, Copyright (c) 2018 Istvan Bartha
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package tasks.ui

import org.scalajs.dom.raw._
import org.scalajs.dom
import scala.scalajs.js.annotation._
import scalatags.JsDom.all._
import akka.ui._
import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import tasks.shared.CPUMemoryAvailable

class AppUI(container: Node) {

  implicit val system = ActorSystem("AkkaUI")
  implicit val materializer = ActorMaterializer()

  val host = dom.document.location.host

  val TableHeader = tr(th("ID"), th("CPU"), th("RAM"))

  def makeTable(header: String,
                project: UIAppState => Seq[(UIJobId, CPUMemoryAvailable)]) = {
    val body = tbody().render
    val bodySink = Flow[UIAppState]
      .map { uiState =>
        project(uiState)
          .map {
            case (UIJobId(jobId), resource) =>
              tr(td(`class` := "collapsing")(jobId),
                 td(resource.cpu),
                 td(resource.memory)).render
          }
      }
      .to(body.childrenSink)
    val element =
      table(`class` := "ui celled table")(
        thead(tr(th(colspan := "3")(header)), TableHeader),
        body)
    (element, bodySink)
  }

  val (runningTable, runningSink) = makeTable("Running nodes", _.running)
  val (pendingTable, pendingSink) = makeTable("Pending nodes", _.pending)

  val (cumulativeCountElem, cumulativeSink) = {
    val elem = div().render
    val sink = Flow[UIAppState]
      .map(state => Seq(span(state.cumulativeRequested.toString).render))
      .to(elem.childrenSink)
    (div(`class` := "ui red horizontal label")("Cumulative requested nodes: ",
                                               elem),
     sink)
  }

  val root = div(
    div(`class` := "ui grid container")(
      div(`class` := "two wide column")(cumulativeCountElem),
      div(`class` := "eight wide column")(runningTable),
      div(`class` := "eight wide column")(pendingTable),
    )
  )

  val (wsSource, wsSink) = WebSocketHelper.open(s"ws://$host/states")

  val uiStateSource = wsSource
    .map(wsMessage =>
      io.circe.parser.decode[UIAppState](wsMessage.data.toString).right.get)

  val keepAliveTicks = {
    import scala.concurrent.duration._
    Source.tick(0 seconds, 500 milliseconds, "tick")
  }

  keepAliveTicks.runWith(wsSink)

  val combinedUIStateSinks = Sink
    .combine(runningSink, pendingSink, cumulativeSink)(Broadcast[UIAppState](_))

  uiStateSource.runWith(combinedUIStateSinks)

  container.appendChild(root.render)

}

@JSExportTopLevel("tasks.ui.AppMain")
object AppMain {

  @JSExport
  def bind(parent: Node) = {
    new AppUI(parent)
  }

}
