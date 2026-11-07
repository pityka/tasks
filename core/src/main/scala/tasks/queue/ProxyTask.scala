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
import com.github.plokhotnyuk.jsoniter_scala.core._
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
private[tasks] case class Proxy(address: tasks.util.message.Address)

/* Local proxy of the remotely executed task */
private[tasks] class ProxyTask[Input, Output](
    taskId: TaskId,
    inputDeserializer: Spore[Unit, Deserializer[Input]],
    outputSerializer: Spore[Unit, Serializer[Output]],
    function: Spore[Input, ComputationEnvironment => IO[Output]],
    input: Input,
    writer: Serializer[Input],
    filePrefix: FilePrefix[Input],
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
    messenger: Messenger,
    sessionId: String,
    config: tasks.util.config.TasksConfig,
    fileServiceComponent: FileServiceComponent
) extends tasks.util.Actor.ActorBehavior[Proxy](messenger) {
  val address: Address = Address(
    tasks.util.SessionId.tag(
      sessionId,
      s"ProxyTask-$taskId-${input.hashCode()}-${scala.util.Random.alphanumeric.take(22).mkString}"
    )
  ).withAddress(messenger.listeningAddress)
  def derive(): Proxy = Proxy(address)

  private def distributeResult(result: Output) = {
    scribe.debug("Completing promise.", address)
    promise.complete(Right(result))
  }

  private def notifyListenersOnFailure(cause: Throwable) =
    promise.complete(Left(cause))

  private def startTask(cache: Boolean) = {

    val persisted: Option[Input] = input match {
      case x: HasPersistent[_] => Some(x.persistent.asInstanceOf[Input])
      case _                   => None
    }

    val hashInput = persisted.getOrElse(input)

    val serializedInput = writer(input)

    val scheduleTask = writer.hash(hashInput).flatMap { dataHash =>
      val prefix = filePrefix.prefix(hashInput)
      val inputDataIO =
        if (serializedInput.length.toLong <= config.maxInlinePayloadBytes)
          IO.pure(
            MessageData.InputData(Base64DataHelpers(serializedInput), noCache)
          )
        else
          SharedFileHelper
            .createFromStream(
              fs2.Stream.chunk(fs2.Chunk.array(serializedInput)),
              "__meta__input__" + dataHash
            )(
              fileServicePrefix,
              fileServiceComponent,
              config,
              tasks.fileservice.NoHistory
            )
            .map(sf =>
              MessageData.InputData(
                Base64DataHelpers(Array.emptyByteArray),
                noCache,
                Some(sf)
              )
            )
      inputDataIO.map { inputData =>
        MessageData.ScheduleTask(
          description = HashedTaskDescription(taskId, dataHash),
          inputDeserializer = inputDeserializer.as[AnyRef, AnyRef],
          outputSerializer = outputSerializer.as[AnyRef, AnyRef],
          function = function.as[AnyRef, AnyRef],
          resource = resourceConsumed,
          input = inputData,
          fileServicePrefix = fileServicePrefix,
          tryCache = cache,
          priority = priority,
          labels = labels,
          lineage = lineage,
          proxy = address,
          filePrefix = prefix.value
        )
      }
    }

    scheduleTask.flatMap { scheduleTask =>
      scribe.debug(
        "proxy submitting ScheduleTask object to queue.",
        scheduleTask,
        address
      )
      queue.scheduleTask(scheduleTask)
    }

  }

  private def resolveResult(result: UntypedResult): IO[UntypedResult] =
    result.spilled match {
      case None => IO.pure(result)
      case Some(sf) =>
        SharedFileHelper
          .stream(sf, 0L)(fileServiceComponent)
          .compile
          .to(Array)
          .map(bytes =>
            readFromArray[UntypedResult](bytes)(UntypedResult.codec)
          )
    }

  private def handleResult(
      result: QueueImpl.ProxyResult,
      stopQueue: Actor.StopQueue
  ): IO[Unit] = result match {
    case QueueImpl.ProxyResultFailure(cause) =>
      IO(scribe.error(cause, "Execution failed. ")) *>
        notifyListenersOnFailure(cause) *> stopProcessingMessages(stopQueue)
    case QueueImpl.ProxyResultSuccess(untypedOutput, retrievedFromCache) =>
      resolveResult(untypedOutput).flatMap { resolved =>
        reader(Base64DataHelpers.toBytes(resolved.data)) match {
          case Right(output) =>
            IO(
              scribe.debug(s"Result polled from queue. $resolved, $output")
            ) *> distributeResult(output) *> stopProcessingMessages(stopQueue)
          case Left(error) if retrievedFromCache =>
            IO(
              scribe.error(
                s"Result from cache failed to decode: $resolved, $error. Task is rescheduled without caching."
              )
            ) *> startTask(cache = false)
          case Left(error) =>
            IO(
              scribe.error(
                error,
                s"Result not from cache failed to decode: $resolved, $error. Execution failed."
              )
            ) *> notifyListenersOnFailure(new RuntimeException(error)) *>
              stopProcessingMessages(stopQueue)
        }
      }
  }

  override def schedulers(
      stopQueue: Actor.StopQueue
  ): Option[IO[fs2.Stream[IO, Unit]]] = Some(IO {

    def submitUntilAccepted: IO[Unit] =
      startTask(cache = true).handleErrorWith { e =>
        IO(
          scribe.warn(
            e,
            "Failed to submit this task to the queue. Retrying, the caller stays blocked until it succeeds.",
            address
          )
        ) *> IO.sleep(config.askInterval) *> submitUntilAccepted
      }

    val submit = fs2.Stream.eval(submitUntilAccepted) ++ fs2.Stream.never[IO]

    val poll = fs2.Stream
      .fixedDelay[IO](config.resultPollInterval)
      .evalMap(_ =>
        queue
          .pollResult(address)
          .flatMap {
            case None         => IO.unit
            case Some(result) => handleResult(result, stopQueue)
          }
          .handleErrorWith { e =>
            IO(
              scribe.warn(
                e,
                "Failed to poll the queue for the result of this task. Retrying on the next poll.",
                address
              )
            )
          }
      )
    submit.merge(poll)
  })

  def receive = (stopQueue) => {

    case Message(
          MessageData.MessageFromTask(untypedOutput, retrievedFromCache),
          from,
          _
        ) =>
      resolveResult(untypedOutput).flatMap { resolved =>
        reader(Base64DataHelpers.toBytes(resolved.data)) match {
          case Right(output) =>
            scribe.debug(
              s"MessageFromTask received from: $from, $resolved, $output"
            )
            distributeResult(output) *> stopProcessingMessages(stopQueue)
          case Left(error) if retrievedFromCache =>
            scribe.error(
              s"MessageFromTask received from cache and failed to decode: ${from}, $resolved, $error. Task is rescheduled without caching."
            )
            startTask(cache = false)
          case Left(error) =>
            scribe.error(
              error,
              s"MessageFromTask received not from cache and failed to decode: ${from}, $resolved, $error. Execution failed."
            )
            notifyListenersOnFailure(
              new RuntimeException(error)
            ) *> stopProcessingMessages(stopQueue)
        }
      }

    case Message(MessageData.TaskFailedMessageToProxy(_, cause), _, _) =>
      scribe.error(cause, "Execution failed. ")
      notifyListenersOnFailure(cause) *> stopProcessingMessages(stopQueue)
  }

}
