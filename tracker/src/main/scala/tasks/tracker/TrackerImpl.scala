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

package tasks.tracker

import akka.actor._
import akka.stream.scaladsl._
import akka.stream._
import tasks.queue.TaskQueue
import tasks.ui.EventListener
import tasks.util.config.TasksConfig
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import tasks.queue.{TaskId, ResultMetadata}
import tasks.shared.ResourceAllocated
import tasks.shared.{ElapsedTimeNanoSeconds, Labels}
import tasks.fileservice.SharedFile

case class ResourceUtilizationRecord(taskId: TaskId,
                                     labels: Labels,
                                     elapsedTime: ElapsedTimeNanoSeconds,
                                     resource: ResourceAllocated,
                                     metadata: Option[ResultMetadata],
                                     resultFiles: Option[Set[SharedFile]],
                                     codeVersion: Option[String])

object ResourceUtilizationRecord {
  implicit val encoder: Encoder[ResourceUtilizationRecord] =
    deriveEncoder[ResourceUtilizationRecord]
  implicit val decoder: Decoder[ResourceUtilizationRecord] =
    deriveDecoder[ResourceUtilizationRecord]
}

class TrackerImpl(implicit actorSystem: ActorSystem, config: TasksConfig)
    extends Tracker {

  implicit val AM = ActorMaterializer()

  val log = akka.event.Logging(actorSystem.eventStream, getClass)
  log.info("Instantiating resource tracking")

  private val stateFlow =
    Flow[TaskQueue.Event]
      .collect {
        case taskDone: TaskQueue.TaskDone =>
          taskDone
      }

  private val sink = Flow[TaskQueue.TaskDone]
    .map { td =>
      val dto = ResourceUtilizationRecord(
        td.sch.description.taskId,
        td.sch.labels,
        td.elapsedTime,
        td.resourceAllocated,
        Some(td.result.metadata),
        Some(td.result.untypedResult.files),
        Some(config.codeVersion)
      )
      import io.circe.syntax._
      akka.util.ByteString(dto.asJson.noSpaces + "\n")
    }
    .to(FileIO
      .toPath(
        new java.io.File(config.resourceUtilizationLogFile).toPath,
        options = Set(
          java.nio.file.StandardOpenOption.APPEND,
          java.nio.file.StandardOpenOption.WRITE,
          java.nio.file.StandardOpenOption.CREATE,
          java.nio.file.StandardOpenOption.SYNC
        )
      ))

  private val (eventListenerActor, eventSource) =
    ActorSource.make[TaskQueue.Event]

  eventSource
    .via(stateFlow)
    .runWith(sink)

  def eventListener: EventListener[TaskQueue.Event] =
    new EventListener[TaskQueue.Event] {
      def watchable = eventListenerActor

      def close =
        eventListenerActor ! PoisonPill

      def receive(event: TaskQueue.Event): Unit = {
        eventListenerActor ! event
      }
    }

}
