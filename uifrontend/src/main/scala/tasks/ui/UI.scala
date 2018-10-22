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
import scalatags.JsDom.all._
import akka.ui._
import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import scala.concurrent.ExecutionContext.Implicits.global

import tasks.queue._
import tasks.shared._

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

object Helpers {

  def prettyJson(b64: Base64Data): String =
    io.circe.parser
      .parse(new String(java.util.Base64.getDecoder.decode(b64.value)))
      .fold(_.toString, _.spaces2)

  def showUILauncher(launcher: UILauncherActor): String = {
    val url = new java.net.URI(launcher.actorPath)
    Option(url.getHost).getOrElse("local") + ":" + Option(url.getPort)
      .getOrElse("")

  }

  def renderTable(render: UIQueueState => Seq[dom.raw.Element])(
      implicit AS: ActorSystem) = {
    val t = table(`class` := "ui celled table").render
    val tSink = Flow[UIQueueState]
      .map(render)
      .to(t.childrenSink)
    (t, tSink)
  }

  val ScheduledTasksTableHeader = tr(th("ID"),
                                     th("Input"),
                                     th("Launcher"),
                                     th("CodeVersion"),
                                     th("CPU"),
                                     th("RAM"))

  def renderTableBodyWithScheduledTasks(
      scheduledTasks: List[(TaskDescription,
                            (UILauncherActor, VersionedResourceAllocated))]) =
    tbody(
      scheduledTasks.toSeq.map {
        case ((taskDescription, (launcher, resource))) =>
          tr(
            td(`class` := "collapsing")(
              taskDescription.taskId.id + " @" + taskDescription.taskId.version
            ),
            td(
              code(
                new String(java.util.Base64.getDecoder
                  .decode(taskDescription.input.value)))
            ),
            td(showUILauncher(launcher)),
            td(resource.codeVersion),
            td(resource.cpu),
            td(resource.memory),
          )
      }
    ).render

  val CompletedTasksTableHeader = tr(th("ID"),
                                     th("Input"),
                                     th("Launcher"),
                                     th("CodeVersion"),
                                     th("CPU"),
                                     th("RAM"),
                                     th("Result"),
                                     th("ResultFiles"))

  val RecoveredTasksTableHeader =
    tr(th("ID"), th("Input"), th("Result"), th("ResultFiles"))

  def renderTableBodyWithCompletedTasks(
      completedTasks: List[(TaskDescription,
                            (UILauncherActor, VersionedResourceAllocated),
                            UIUntypedResult)]) =
    tbody(
      completedTasks.toSeq.map {
        case ((taskDescription, (launcher, resource), result)) =>
          tr(
            td(`class` := "collapsing")(
              taskDescription.taskId.id + " @" + taskDescription.taskId.version
            ),
            td(
              code(prettyJson(taskDescription.input))
            ),
            td(showUILauncher(launcher)),
            td(resource.codeVersion),
            td(resource.cpu),
            td(resource.memory),
            td(code(prettyJson(result.data))),
            td(result.files.toString)
          )
      }
    ).render

  def renderTableBodyWithRecoveredTasks(
      recoveredTasks: List[(TaskDescription, UIUntypedResult)]) =
    tbody(
      recoveredTasks.toSeq.map {
        case ((taskDescription, result)) =>
          tr(
            td(`class` := "collapsing")(
              taskDescription.taskId.id + " @" + taskDescription.taskId.version
            ),
            td(
              code(prettyJson(taskDescription.input))
            ),
            td(code(prettyJson(result.data))),
            td(result.files.toString)
          )
      }
    ).render

}
