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

import tasks.fileservice._
import tasks.shared._
import tasks._
import tasks.wire._
import scala.concurrent.Promise
import cats.effect.IO
import tasks.caching.TaskResultCache
import tasks.util.message.MessageData
import tasks.util.Messenger
import tasks.util.message.Message
import tasks.util.message.Address
import cats.effect.unsafe.implicits.global
import cats.effect.kernel.Deferred
import cats.effect.kernel.Ref
import tasks.util.message._
import tasks.util.Actor
case class Proxy(address: tasks.util.message.Address)

/* Local proxy of the remotely executed task */
private[tasks] class ProxyTask[Input, Output](
    taskId: TaskId,
    inputDeserializer: Spore[Unit, Deserializer[Input]],
    outputSerializer: Spore[Unit, Serializer[Output]],
    function: Spore[Input, ComputationEnvironment => IO[Output]],
    input: Input,
    writer: Serializer[Input],
    reader: Deserializer[Output],
    resourceConsumed: VersionedResourceRequest,
    queue: Queue,
    fileServicePrefix: FileServicePrefix,
    cache: TaskResultCache,
    priority: Priority,
    promise: Deferred[IO, Either[Throwable, Output]],
    labels: Labels,
    lineage: TaskLineage,
    noCache: Boolean,
    messenger: Messenger
) extends tasks.util.Actor.ActorBehavior[ Proxy](messenger) {
  val address: Address = Address(
    s"ProxyTask-$taskId-${input.hashCode()}-${scala.util.Random.alphanumeric.take(256).mkString}"
  )
  def derive(): Proxy = Proxy(address)

  private def distributeResult(result: Output) = {
    scribe.debug("Completing promise.")
    promise.complete(Right(result))
  }

  private def notifyListenersOnFailure(cause: Throwable) =
    promise.complete(Left(cause))

  private def startTask(cache: Boolean) = {

    val persisted: Option[Input] = input match {
      case x: HasPersistent[_] => Some(x.persistent.asInstanceOf[Input])
      case _                   => None
    }

    val hash: IO[HashedTaskDescription] = writer
      .hash(persisted.getOrElse(input))
      .map(hash =>
        HashedTaskDescription(
          taskId,
          hash
        )
      )

    val scheduleTask = hash.map { hash =>
      MessageData.ScheduleTask(
        description = hash,
        inputDeserializer = inputDeserializer.as[AnyRef, AnyRef],
        outputSerializer = outputSerializer.as[AnyRef, AnyRef],
        function = function.as[AnyRef, AnyRef],
        resource = resourceConsumed,
        input =
          MessageData.InputData(Base64DataHelpers(writer(input)), noCache),
        fileServicePrefix = fileServicePrefix,
        tryCache = cache,
        priority = priority,
        labels = labels,
        lineage = lineage,
        proxy = address
      )
    }

    scribe.debug("proxy submitting ScheduleTask object to queue.")

    scheduleTask.flatMap { scheduleTask =>
      queue.scheduleTask(scheduleTask)
    }

  }

  override def schedulers(
      
      stopQueue: Actor.StopQueue
  ): Option[IO[fs2.Stream[IO, Unit]]] = Some(IO {
    (fs2.Stream.unit ++ fs2.Stream.never[IO]).evalMap(_ =>
      startTask(cache = true)
    )
  })

  def receive = (stopQueue) => {

    case Message(
          MessageData.MessageFromTask(untypedOutput, retrievedFromCache),
          from,
          _
        ) =>
      reader(Base64DataHelpers.toBytes(untypedOutput.data)) match {
        case Right(output) =>
          scribe.debug(
            s"MessageFromTask received from: $from, $untypedOutput, $output"
          )
          distributeResult(output) *> stopProcessingMessages(stopQueue)
        case Left(error) if retrievedFromCache =>
          scribe.error(
            s"MessageFromTask received from cache and failed to decode: ${from}, $untypedOutput, $error. Task is rescheduled without caching."
          )
          startTask(cache = false)
        case Left(error) =>
          scribe.error(
            error,
            s"MessageFromTask received not from cache and failed to decode: ${from}, $untypedOutput, $error. Execution failed."
          )
           notifyListenersOnFailure(
            new RuntimeException(error)
          ) *> stopProcessingMessages(stopQueue)
      }

    case Message(MessageData.TaskFailedMessageToProxy(_, cause), _, _) =>
      scribe.error(cause, "Execution failed. ")
      notifyListenersOnFailure(cause) *> stopProcessingMessages(stopQueue)
  }

}
