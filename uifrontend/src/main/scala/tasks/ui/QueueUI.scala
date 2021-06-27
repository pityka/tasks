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
import com.github.plokhotnyuk.jsoniter_scala.core._
@JSExportTopLevel("QueueMain")
object QueueMain {

  @JSExport
  def bind(container: dom.raw.Element) = {

    import Helpers._

    val host = dom.document.location.host

    val websocket = WebSocketHelper.open(s"ws://$host/states")

    val uiStateSource = websocket.events
      .map(wsMessage => readFromString[UIQueueState](wsMessage))

    val knownLaunchersTable = {
      val body = tbody(
        children <-- uiStateSource.map { uiState =>
          uiState.knownLaunchers.toList
            .map(showUILauncher)
            .map(actorPath => tr(td(cls := "collapsing", actorPath)))
        }
      )

      val element =
        table(cls := "ui celled table", thead(tr(th("Launchers"))), body)
      element
    }

    val (scheduledTasksTable) = renderTable(
      uiState =>
        List(
          thead(
            tr(
              th(
                colSpan := 6,
                "Scheduled tasks, total: " + uiState.scheduledTasks.size
              )
            ),
            ScheduledTasksTableHeader
          ),
          renderTableBodyWithScheduledTasks(uiState.scheduledTasks)
        ),
      uiStateSource
    )

    val (completedTasksTable) = renderTable(
      uiState =>
        List(
          thead(
            tr(
              th(
                colSpan := 8,
                "Completed tasks, total: " + uiState.completedTasks.toSeq
                  .map(_._2)
                  .sum
              )
            ),
            CompletedTasksTableHeader
          ),
          renderTableBodyWithCompletedTasks(uiState.completedTasks)
        ),
      uiStateSource
    )

    val (recoveredTasksTable) = renderTable(
      uiState =>
        List(
          thead(
            tr(
              th(
                colSpan := 4,
                "Previously completed tasks, total: " + uiState.recoveredTasks
                  .map(_._2)
                  .sum
              )
            ),
            RecoveredTasksTableHeader
          ),
          renderTableBodyWithRecoveredTasks(uiState.recoveredTasks)
        ),
      uiStateSource
    )

    val (scheduledTasksSummaryTable) =
      renderTable(
        { uiState =>
          val counts = uiState.scheduledTasks
            .map { case (taskDescription, _) => taskDescription.taskId }
            .groupBy(identity)
            .map { case (key, group) => (key, group.size) }
          List(
            thead(
              tr(
                th(
                  colSpan := 4,
                  "Scheduled tasks, total: " + counts
                    .map(_._2)
                    .sum
                )
              ),
              RecoveredTasksTableHeader
            ),
            renderTableBodyWithRecoveredTasks(counts.toSet)
          )
        },
        uiStateSource
      )

    val (failedTasksTable) = renderTable(
      uiState =>
        List(
          thead(
            tr(
              th(
                colSpan := 6,
                "Failed tasks, total: " + uiState.failedTasks.size
              )
            ),
            ScheduledTasksTableHeader
          ),
          renderTableBodyWithScheduledTasks(uiState.failedTasks)
        ),
      uiStateSource
    )

    val (queuedTasksTable) = renderTable(
      uiState =>
        List(
          thead(
            tr(
              th(
                colSpan := 3,
                "Queued tasks, total " + uiState.queuedTasks.size
              )
            ),
            tr(th(), th("ID"), th("Input"))
          ),
          tbody(
            uiState.queuedTasks.toSeq.zipWithIndex
              .map { case (taskDescription, idx) =>
                tr(
                  td(cls := "collapsing", idx),
                  td(
                    cls := "collapsing",
                    taskDescription.taskId.id + " @" + taskDescription.taskId.version
                  ),
                  td(
                    code(taskDescription.hash)
                  )
                )
              }
              .take(10)
          )
        ),
      uiStateSource
    )

    val root = div(
      div(
        cls := "ui",
        div(cls := "", knownLaunchersTable),
        div(cls := "", scheduledTasksSummaryTable),
        div(cls := "", completedTasksTable),
        div(cls := "", recoveredTasksTable),
        div(cls := "", queuedTasksTable),
        div(cls := "", failedTasksTable),
        div(cls := "", scheduledTasksTable)
      )
    )

    render(container, root)

  }

}
