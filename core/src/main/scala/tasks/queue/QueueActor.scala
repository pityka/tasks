/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Modified work, Copyright (c) 2016 Istvan Bartha
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

package tasks.queue

import tasks.util.eq._
import tasks.shared._
import tasks.util._
import tasks.util.config._
import tasks.wire._
import tasks._
import tasks.caching.TaskResultCache
import cats.effect.IO
import tasks.caching.AnswerFromCache
import cats.effect.FiberIO
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import tasks.util.Actor.ActorBehavior
// import tasks.elastic.RemoteNodeRegistry
import tasks.util.message._
import tasks.util.message.MessageData.ScheduleTask
import tasks.util.message.MessageData.Schedule
import tasks.util.message.MessageData.NothingForSchedule
import tasks.util.message.LauncherName

private[tasks] case class QueueActor(
    val address0: Address
)

private[tasks] object QueueActor {
  val singletonAddress = Address("TasksQueue")
  val referenceByAddress = QueueActor(singletonAddress)
  def makeReference(
      messenger: Messenger
  )(implicit config: TasksConfig): IO[Either[Throwable, QueueActor]] = Ask
    .ask(
      target = singletonAddress,
      data = MessageData.Ping,
      timeout = config.pendingNodeTimeout,
      messenger = messenger
    )
    .map {
      case Right(Some(_)) => (Right(referenceByAddress))
      case Right(None) => (
        Left(new RuntimeException(s"QueueActor not reachable"))
      )
      case Left(e) => Left(e)
    }

  def makeWithQueueImpl(
      impl: QueueImpl,
      cache: TaskResultCache,
      messenger: Messenger
  )(implicit
      config: TasksConfig
  ): Resource[IO, QueueActor] = {
    util.Actor.makeFromBehavior(
      new QueueActorBehavior(impl, messenger, cache),
      messenger
    )
  }
}

private[tasks] final class QueueActorBehavior(
    impl: QueueImpl,
    messenger: Messenger,
    cache: TaskResultCache
)(implicit config: TasksConfig)
    extends ActorBehavior[ QueueActor](messenger) {
  val address: Address = QueueActor.singletonAddress
  
  def derive(): QueueActor = QueueActor(address)

  def receive = ( _) => {
    case Message(sch: ScheduleTask, _, _) =>
      impl.scheduleTask(sch)

    case Message(MessageData.Increment(launcher), _, _) =>
      impl.increment(launcher)
   
    case Message(MessageData.InitFailed(n), _, _) =>
       impl.initFailed(n)

    case Message(
          MessageData.AskForWork(availableResource, launcher, node),
          from,
          _
        ) =>
      impl
        .askForWork(launcher, availableResource, node)
        .flatMap {
          case Left(t) =>
            messenger.submit(
              Message(
                t,
                from = address,
                to = from
              )
            )
          case Right(t) =>
            messenger.submit(
              Message(
                t,
                from = address,
                to = from
              )
            )
        }

    case Message(MessageData.QueueAck(allocated, launcher), _, _) =>
      impl.ack(allocated, launcher)

    case Message(
          MessageData.TaskDone(
            sch,
            resultWithMetadata,
            elapsedTime,
            resourceAllocated
          ),
          _,
          _
        ) =>
      impl.taskSuccess(
        sch,
        resultWithMetadata,
        elapsedTime,
        resourceAllocated
      )

    case Message(MessageData.TaskFailedMessageToQueue(sch, cause), _, _) =>
      impl.taskFailed(sch, cause)

    case Message(MessageData.Ping, from, to) =>
      messenger.submit(
        Message(MessageData.Ping, from = address, to = from)
      )

  

  }

}
