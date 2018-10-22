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
import akka.util.Timeout
import akka.pattern.ask

import scala.concurrent.duration._
import scala.concurrent.Future

import tasks.fileservice._
import tasks.shared._
import tasks._
import tasks.wire._

private[tasks] object ProxyTask {

  def askForResult(actor: ActorRef, timeoutp: FiniteDuration): Future[Any] = {
    implicit val timout = Timeout(timeoutp)
    (actor ? GetBackResult)

  }

}

/* Local proxy of the remotely executed task */
class ProxyTask[Input, Output](
    taskId: TaskId,
    runTaskClass: java.lang.Class[_ <: CompFun2],
    input: Input,
    writer: Serializer[Input],
    reader: Deserializer[Output],
    resourceConsumed: VersionedResourceRequest,
    queueActor: ActorRef,
    fileServiceComponent: FileServiceComponent,
    fileServicePrefix: FileServicePrefix,
    cacheActor: ActorRef
) extends Actor
    with akka.actor.ActorLogging {

  private var listeners: Set[ActorRef] = Set[ActorRef]()

  private var result: Option[Any] = None

  private def distributeResult(): Unit = {
    log.debug(s"Distributing result to listeners: $listeners")
    result.foreach { result =>
      listeners.foreach { listener =>
        listener ! result
      }
    }
  }

  private def notifyListenersOnFailure(cause: Throwable): Unit =
    listeners.foreach(_ ! akka.actor.Status.Failure(cause))

  private def startTask(cache: Boolean): Unit =
    if (result.isEmpty) {

      val persisted: Option[Input] = input match {
        case x: HasPersistent[Input] => Some(x.persistent)
        case _                       => None
      }

      val scheduleTask = ScheduleTask(
        TaskDescription(taskId,
                        Base64DataHelpers(writer(input)),
                        persisted.map(x => Base64DataHelpers(writer(x)))),
        runTaskClass.getName,
        resourceConsumed,
        queueActor,
        fileServiceComponent.actor,
        fileServicePrefix,
        cacheActor,
        cache
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
    case MessageFromTask(untypedOutput) =>
      reader(Base64DataHelpers.toBytes(untypedOutput.data)) match {
        case Right(output) =>
          log.debug("MessageFromTask received from: {}, {}, {},{}",
                    sender,
                    untypedOutput,
                    result,
                    output)
          if (result.isEmpty) {
            result = Some(output)
            distributeResult()
          }
        case Left(error) =>
          log.error(
            s"MessageFromTask received from and failed to decode: $sender, $untypedOutput, $result, $error. Task is rescheduled without caching.")
          startTask(cache = false)
      }

    case GetBackResult =>
      log.debug(
        "GetBackResult message received. Registering for notification: " + sender.toString)
      listeners += sender
      distributeResult()

    case TaskFailedMessageToProxy(_, cause) =>
      log.error(cause, "Execution failed. ")
      notifyListenersOnFailure(cause)
      self ! PoisonPill
  }

}
