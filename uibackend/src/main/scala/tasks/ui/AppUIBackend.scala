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

import tasks.elastic.NodeRegistry
import tasks.util.config.TasksConfig
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import scala.concurrent.duration._
import org.http4s.websocket.WebSocketFrame

class AppUIBackendImpl(implicit config: TasksConfig) extends AppUI {

  private val ref =
    cats.effect.kernel.Ref[IO].of(UIAppState.empty).unsafeRunSync()

  def update(a: NodeRegistry.Event) = ref
    .getAndUpdate(old => UIAppStateProjector.project(old, a))
    .unsafeRunAndForget()

  def makeOutgoingStream = fs2.Stream
    .awakeEvery[IO](1.second)
    .evalMap(_ =>
      ref.get.map { state =>
        WebSocketFrame.Text(writeToString(state))
      }
    )

  // Flow[NodeRegistry.Event]
  //   .scan(UIAppState.empty)(UIAppStateProjector.project(_, _))

  // eventSource
  //   .via(stateFlow)
  //   .runWith(
  //     Sink.actorRef(
  //       multiplex,
  //       onCompleteMessage = None,
  //       onFailureMessage = (_ => ())
  //     )
  //   )

  // private val route =
  //   get {
  //     path("states") {
  //       val source = Source
  //         .actorRef[UIAppState](
  //           completionMatcher = { case akka.actor.Status.Success =>
  //             CompletionStrategy.draining
  //           }: PartialFunction[Any, CompletionStrategy],
  //           failureMatcher = { case akka.actor.Status.Failure(e) =>
  //             e
  //           }: PartialFunction[Any, Throwable],
  //           bufferSize = 100,
  //           overflowStrategy = OverflowStrategy.dropTail
  //         )
  //         .mapMaterializedValue { actorRef =>
  //           multiplex ! actorRef
  //           actorRef
  //         }
  //         .viaMat(stateToTextMessage)(Keep.left)
  //         .watchTermination() { (actorRef, terminationFuture) =>
  //           terminationFuture.foreach { case akka.Done =>
  //             multiplex ! Multiplex.Unsubscribe(actorRef)
  //           }
  //         }
  //       handleWebSocketMessages(Flow.fromSinkAndSource(Sink.ignore, source))
  //     } ~
  //       pathSingleSlash {
  //         Route.seal(getFromResource("public/index_app.html"))

  //       } ~
  //       path("tasks-ui-frontend-fastopt.js") {
  //         Route.seal(getFromResource("tasks-ui-frontend-fastopt.js"))

  //       } ~
  //       path("tasks-ui-frontend-fullopt.js") {
  //         Route.seal(getFromResource("tasks-ui-frontend-fullopt.js"))
  //       } ~
  //       path("jquery@3.3.1.min.js") {
  //         Route.seal(getFromResource("public/jquery.min.js"))
  //       } ~
  //       path("semantic-ui@2.3.3.min.js") {
  //         Route.seal(getFromResource("public/semantic.min.js"))
  //       } ~
  //       path("semantic-ui@2.3.3.min.css") {
  //         Route.seal(getFromResource("public/semantic.min.css"))
  //       }

  //   }

  // private val bindingFuture =
  //   Http()
  //     .newServerAt(config.appUIServerHost, config.appUIServerPort)
  //     .bind(route)

  // bindingFuture.andThen { case scala.util.Success(serverBinding) =>
  //   log.info(
  //     s"Started UI app backend http server at ${serverBinding.localAddress}"
  //   )
  // }

  def nodeRegistryEventListener: EventListener[NodeRegistry.Event] =
    new EventListener[NodeRegistry.Event] {
      def close() = () // eventListener ! PoisonPill
      def receive(event: NodeRegistry.Event): Unit = {
        update(event)
      }
    }

  // def unbind = {
  //   bindingFuture.flatMap(_.unbind())
  // }
}
