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

import akka.actor._
import akka.stream.scaladsl._
import akka.stream._
import tasks.queue.TaskQueue
import tasks.util.config.TasksConfig
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

/* Do not change the fqcn of this object! See core/UIComponent.scala */
object BackendUIBootstrap extends UIComponentBootstrap {
  def start(implicit actorSystem: ActorSystem,
            config: TasksConfig): UIBackendImpl =
    new UIBackendImpl
}

private class Multiplex extends Actor {
  var listeners = List[ActorRef]()
  var last: Option[Any] = None
  def receive = {
    case actor: ActorRef =>
      listeners = actor :: listeners
      last.foreach(last => actor ! last)
    case Multiplex.Unsubscribe(actor) =>
      listeners = listeners.filterNot(_ == actor)
    case other =>
      listeners.foreach(_ ! other)
      last = Some(other)
  }
}
object Multiplex {
  case class Unsubscribe(ac: ActorRef)
}

class UIBackendImpl(implicit actorSystem: ActorSystem, config: TasksConfig)
    extends UIComponent {

  implicit val AM = ActorMaterializer()
  import actorSystem.dispatcher

  val log = akka.event.Logging(actorSystem.eventStream, getClass)

  private val stateFlow =
    Flow[TaskQueue.Event].scan(UIState.empty)(UIStateProjector.project(_, _))

  private val multiplex = actorSystem.actorOf(Props[Multiplex])

  private val (eventListener, eventSource) = ActorSource.make[TaskQueue.Event]

  eventSource
    .via(stateFlow)
    .runWith(Sink.actorRef(multiplex, onCompleteMessage = None))

  private val stateToTextMessage =
    Flow[UIState].map { state =>
      val json: String =
        implicitly[io.circe.Encoder[UIState]].apply(state).noSpaces
      TextMessage(json)
    }

  private val stateWebSocketRoute =
    get {
      path("states") {
        val source = Source
          .actorRef[UIState](bufferSize = 1000000,
                             overflowStrategy = OverflowStrategy.fail)
          .mapMaterializedValue { actorRef =>
            multiplex ! actorRef
            actorRef
          }
          .viaMat(stateToTextMessage)(Keep.left)
          .watchTermination() { (actorRef, terminationFuture) =>
            terminationFuture.foreach {
              case akka.Done =>
                multiplex ! Multiplex.Unsubscribe(actorRef)
            }
          }
        handleWebSocketMessages(Flow.fromSinkAndSource(Sink.ignore, source))
      } ~
        pathSingleSlash {
          Route.seal(getFromResource("public/index.html"))

        } ~
        path("tasks-ui-frontend-fastopt.js") {
          Route.seal(getFromResource("tasks-ui-frontend-fastopt.js"))

        } ~
        path("tasks-ui-frontend-fullopt.js") {
          Route.seal(getFromResource("tasks-ui-frontend-fullopt.js"))
        }

    }

  private val route = stateWebSocketRoute

  private val bindingFuture =
    Http().bindAndHandle(route, config.uiServerHost, config.uiServerPort)

  bindingFuture.andThen {
    case scala.util.Success(serverBinding) =>
      log.info(
        s"Started UI backend http server at ${serverBinding.localAddress}")
  }

  def tasksQueueEventListener: EventListener[TaskQueue.Event] =
    new EventListener[TaskQueue.Event] {
      def receive(event: TaskQueue.Event): Unit = {
        eventListener ! event
      }
    }

  def unbind = {
    bindingFuture.flatMap(_.unbind())
  }
}
