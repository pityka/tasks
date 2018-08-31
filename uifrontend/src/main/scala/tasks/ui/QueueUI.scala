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

class QueueUI(container: Node) {
  import Helpers._

  implicit val system = ActorSystem("AkkaUI")
  implicit val materializer = ActorMaterializer()

  val host = dom.document.location.host

  val (knownLaunchersTable, knownLaunchersSink) = {
    val body = tbody().render
    val bodySink = Flow[UIQueueState]
      .map { uiState =>
        uiState.knownLaunchers.toSeq
          .map(showUILauncher)
          .sorted
          .map(actorPath => tr(td(`class` := "collapsing")(actorPath)).render)
      }
      .to(body.childrenSink)
    val element =
      table(`class` := "ui celled table")(thead(tr(th("Launchers"))), body)
    (element, bodySink)
  }

  val (scheduledTasksTable, scheduledTasksTableSink) = renderTable(
    uiState =>
      List(
        thead(
          tr(
            th(colspan := "6")(
              "Scheduled tasks, total: " + uiState.scheduledTasks.size)),
          ScheduledTasksTableHeader
        ).render,
        renderTableBodyWithScheduledTasks(uiState.scheduledTasks)
    )
  )

  val (completedTasksTable, completedTasksTableSink) = renderTable(
    uiState =>
      List(
        thead(
          tr(
            th(colspan := "8")(
              "Completed tasks, total: " + uiState.completedTasks.size)),
          CompletedTasksTableHeader
        ).render,
        renderTableBodyWithCompletedTasks(uiState.completedTasks)
    )
  )

  val (recoveredTasksTable, recoveredTasksTableSink) = renderTable(
    uiState =>
      List(
        thead(
          tr(th(colspan := "4")(
            "Previously completed tasks, total: " + uiState.recoveredTasks.size)),
          RecoveredTasksTableHeader
        ).render,
        renderTableBodyWithRecoveredTasks(uiState.recoveredTasks)
    )
  )

  val (failedTasksTable, failedTasksTableSink) = renderTable(
    uiState =>
      List(
        thead(
          tr(
            th(colspan := "6")(
              "Failed tasks, total: " + uiState.failedTasks.size)),
          ScheduledTasksTableHeader
        ).render,
        renderTableBodyWithScheduledTasks(uiState.failedTasks)
    )
  )

  val (queuedTasksTable, queuedTasksTableSink) = renderTable(
    uiState =>
      List(
        thead(
          tr(
            th(colspan := "3")(
              "Queued tasks, total " + uiState.queuedTasks.size)),
          tr(th(), th("ID"), th("Input"))
        ).render,
        tbody(
          uiState.queuedTasks.toSeq.zipWithIndex
            .map {
              case (taskDescription, idx) =>
                tr(
                  td(`class` := "collapsing")(idx),
                  td(`class` := "collapsing")(
                    taskDescription.taskId.id + " @" + taskDescription.taskId.version
                  ),
                  td(
                    code(prettyJson(taskDescription.input))
                  )
                )
            }
            .map(_.render)
            .take(10)).render
    )
  )

  val root = div(
    div(`class` := "ui grid container")(
      div(`class` := "two wide column")(knownLaunchersTable),
      div(`class` := "eight wide column")(queuedTasksTable),
      div(`class` := "twelve wide column centered")(failedTasksTable),
      div(`class` := "twelve wide column centered")(scheduledTasksTable),
      div(`class` := "twelve wide column centered")(completedTasksTable),
      div(`class` := "twelve wide column centered")(recoveredTasksTable)
    )
  )

  val (wsSource, wsSink) = WebSocketHelper.open(s"ws://$host/states")

  val uiStateSource = wsSource
    .map(wsMessage =>
      io.circe.parser.decode[UIQueueState](wsMessage.data.toString).right.get)

  val keepAliveTicks = {
    import scala.concurrent.duration._
    Source.tick(0 seconds, 500 milliseconds, "tick")
  }

  keepAliveTicks.runWith(wsSink)

  val combinedUIStateSinks = Sink
    .combine(knownLaunchersSink,
             queuedTasksTableSink,
             scheduledTasksTableSink,
             completedTasksTableSink,
             failedTasksTableSink,
             recoveredTasksTableSink)(Broadcast[UIQueueState](_))

  uiStateSource.runWith(combinedUIStateSinks)

  container.appendChild(root.render)

}

@JSExportTopLevel("tasks.ui.QueueMain")
object QueueMain {

  @JSExport
  def bind(parent: Node) = {
    new QueueUI(parent)
  }

}
