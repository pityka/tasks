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

  def showUILauncher(launcher: UILauncherActor): String = {
    val url = new java.net.URI(launcher.actorPath)
    Option(url.getHost).getOrElse("local") + ":" + Option(url.getPort)
      .getOrElse("")

  }

  def renderTable(render: UIState => Seq[dom.raw.Element])(
      implicit AS: ActorSystem) = {
    val t = table(`class` := "ui celled table").render
    val tSink = Flow[UIState]
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
                            (UILauncherActor, VersionedCPUMemoryAllocated))]) =
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

  def renderTableBodyWithCompletedTasks(
      completedTasks: List[(TaskDescription,
                            (UILauncherActor, VersionedCPUMemoryAllocated),
                            UIUntypedResult)]) =
    tbody(
      completedTasks.toSeq.map {
        case ((taskDescription, (launcher, resource), result)) =>
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
            td(
              code(new String(
                java.util.Base64.getDecoder.decode(result.data.value)))),
            td(result.files.toString)
          )
      }
    ).render

}

class UI(container: Node) {
  import Helpers._

  implicit val system = ActorSystem("AkkaUI")
  implicit val materializer = ActorMaterializer()

  val host = dom.document.location.host

  val (knownLaunchersTable, knownLaunchersSink) = {
    val body = tbody().render
    val bodySink = Flow[UIState]
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
                    code(new String(java.util.Base64.getDecoder.decode(
                      taskDescription.input.value)))
                  )
                )
            }
            .map(_.render)
            .take(10)).render
    )
  )

  val root = div(
    div(`class` := "ui grid")(
      div(`class` := "two wide column")(knownLaunchersTable),
      div(`class` := "eight wide column")(queuedTasksTable),
      div(`class` := "twelve wide column centered")(failedTasksTable),
      div(`class` := "twelve wide column centered")(scheduledTasksTable),
      div(`class` := "twelve wide column centered")(completedTasksTable)
    )
  )

  val (wsSource, wsSink) = WebSocketHelper.open(s"ws://$host/states")

  val uiStateSource = wsSource
    .map(wsMessage =>
      io.circe.parser.decode[UIState](wsMessage.data.toString).right.get)

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
             failedTasksTableSink)(Broadcast[UIState](_))

  uiStateSource.runWith(combinedUIStateSinks)

  container.appendChild(root.render)

}

@JSExportTopLevel("tasks.ui.Main")
object Main {

  @JSExport
  def bind(parent: Node) = {
    new UI(parent)
  }

}
