/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Modified work, Copyright (c) 2016 Istvan Bartha

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

package tasks.queue

import akka.actor.{Actor, PoisonPill, ActorRef}

import tasks.fileservice._
import tasks.shared._
import tasks._
import tasks.wire._
import scala.concurrent.Promise
import cats.effect.IO

/* Local proxy of the remotely executed task */
class ProxyTask[Input, Output](
    taskId: TaskId,
    inputDeserializer: Spore[Unit, Deserializer[Input]],
    outputSerializer: Spore[Unit, Serializer[Output]],
    function: Spore[Input, ComputationEnvironment => IO[Output]],
    input: Input,
    writer: Serializer[Input],
    reader: Deserializer[Output],
    resourceConsumed: VersionedResourceRequest,
    queueActor: ActorRef,
    fileServicePrefix: FileServicePrefix,
    cacheActor: ActorRef,
    priority: Priority,
    promise: Promise[Output],
    labels: Labels,
    lineage: TaskLineage,
    noCache: Boolean
) extends Actor
    with akka.actor.ActorLogging {

  private def distributeResult(result: Output): Unit = {
    log.debug("Completing promise.")
    promise.success(result)
  }

  private def notifyListenersOnFailure(cause: Throwable): Unit =
    promise.failure(cause)

  private def startTask(cache: Boolean): Unit = {

    val persisted: Option[Input] = input match {
      case x: HasPersistent[_] => Some(x.persistent.asInstanceOf[Input])
      case _                       => None
    }

    val hash: HashedTaskDescription =
      HashedTaskDescription(
        taskId,
        writer.hash(persisted.getOrElse(input))
      )

    val scheduleTask = ScheduleTask(
      hash,
      inputDeserializer.as[AnyRef, AnyRef],
      outputSerializer.as[AnyRef, AnyRef],
      function.as[AnyRef, AnyRef],
      resourceConsumed,
      queueActor,
      fileServicePrefix,
      cacheActor,
      cache,
      priority,
      labels,
      lineage,
      self
    )

    log.debug("proxy submitting ScheduleTask object to queue.")

    queueActor ! scheduleTask
  }

  override def preStart() = {
    log.debug("ProxyTask prestart.")
    startTask(cache = true)

  }

  override def postStop() = {
    log.debug("ProxyTask stopped. {} {} {}", taskId, input, self)
  }

  def receive = {
    case NeedInput =>
      sender() ! InputData(Base64DataHelpers(writer(input)), noCache)
    case MessageFromTask(untypedOutput, retrievedFromCache) =>
      reader(Base64DataHelpers.toBytes(untypedOutput.data)) match {
        case Right(output) =>
          log.debug(
            "MessageFromTask received from: {}, {}, {}",
            sender(),
            untypedOutput,
            output
          )
          distributeResult(output)
          self ! PoisonPill
        case Left(error) if retrievedFromCache =>
          log.error(
            s"MessageFromTask received from cache and failed to decode: ${sender()}, $untypedOutput, $error. Task is rescheduled without caching."
          )
          startTask(cache = false)
        case Left(error) =>
          log.error(
            error,
            s"MessageFromTask received not from cache and failed to decode: ${sender()}, $untypedOutput, $error. Execution failed."
          )
          notifyListenersOnFailure(new RuntimeException(error))
          self ! PoisonPill
      }

    case TaskFailedMessageToProxy(_, cause) =>
      log.error(cause, "Execution failed. ")
      notifyListenersOnFailure(cause)
      self ! PoisonPill
  }

}
