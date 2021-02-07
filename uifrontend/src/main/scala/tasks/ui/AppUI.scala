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

import org.scalajs.dom
import scala.scalajs.js.annotation._
import com.raquo.laminar.api.L._
import tasks.shared.ResourceAvailable
@JSExportTopLevel("AppMain")
object AppMain {

  @JSExport
  def bind(container: dom.raw.Element) {

    val host = dom.document.location.host

    val TableHeader = tr(th("ID"), th("CPU"), th("RAM"))

    def makeTable(
        header: String,
        project: UIAppState => List[(UIJobId, ResourceAvailable)],
        signal: EventStream[UIAppState]
    ) = {
      val body = tbody(children <-- signal.map { uiState =>
        project(uiState)
          .map {
            case (UIJobId(jobId), resource) =>
              tr(
                td(cls := "collapsing", jobId),
                td(resource.cpu),
                td(resource.memory)
              )
          }
      })

      val element =
        table(
          cls := "ui celled table",
          thead(tr(th(colSpan := 3, header)), TableHeader),
          body
        )
      element
    }

    val websocket = WebSocketHelper
      .open(s"ws://$host/states")
    val events = websocket.events
      .map(
        wsMessage => io.circe.parser.decode[UIAppState](wsMessage).right.get
      )

    val runningTable = makeTable("Running nodes", _.running.toList, events)
    val pendingTable = makeTable("Pending nodes", _.pending.toList, events)

    val cumulativeCountElem =
      div(
        cls := "ui red horizontal label",
        "Cumulative requested nodes: ",
        div(
          children <-- events
            .map(state => List(span(state.cumulativeRequested.toString)))
        )
      )

    val root = div(
      div(
        cls := "ui grid container",
        div(cls := "two wide column", cumulativeCountElem),
        div(cls := "eight wide column", runningTable),
        div(cls := "eight wide column", pendingTable)
      )
    )

    render(container, root)

  }

}
