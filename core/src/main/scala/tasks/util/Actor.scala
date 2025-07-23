package tasks.util

import cats.effect.kernel.Ref
import cats.effect.IO
import cats.effect.FiberIO
import cats.effect.kernel.Resource
import cats.effect.std.Queue
import tasks.util.message._
private[tasks] object Actor {
  type StopQueue = Queue[IO, Option[Unit]]

  abstract class ActorBehavior[K](messenger: Messenger) {
    type ReceiveIO =
      (
          StopQueue
      ) => PartialFunction[Message, (IO[Unit])]
    def receive: ReceiveIO

    def sendTo(target: Address, msg: MessageData) =
      messenger.submit(Message(msg, from = address, to = target))
    def schedulers(
        stop: StopQueue
    ): Option[IO[fs2.Stream[IO, Unit]]] = None
    val address: Address
    def derive(): K

    def stopProcessingMessages(stopQueue: StopQueue) = stopQueue.offer(None)

  }

  def makeFromBehavior[K](
      b: ActorBehavior[K],
      messenger: Messenger
  ): Resource[IO, K] = {
    val a = b.address
    make(
      address = a,
      schedulers = b.schedulers _,
      receive = b.receive,
      messenger = messenger
    ).map { case stateRef => b.derive() }
  }

  def make(
      schedulers: (
          StopQueue
      ) => Option[IO[fs2.Stream[IO, Unit]]],
      receive: (
          StopQueue
      ) => PartialFunction[Message, IO[Unit]],
      address: Address,
      messenger: Messenger
  ): Resource[IO, Unit] = {
    Resource.eval(Queue.bounded[IO, Option[Unit]](1)).flatMap { stopQueue =>
      val messageStream = messenger.subscribe(address).map { stream =>
        stream
          .evalMap { message =>
            receive(stopQueue)
              .lift(message)
              .getOrElse(IO.unit)

          }
      }
      val stopStream = fs2.Stream
        .fromQueueNoneTerminated[IO, Unit](stopQueue, 1)
        .map(_ => ())
      val schedulerStream =
        schedulers(stopQueue)
          .getOrElse(IO.pure(fs2.Stream.never[IO]))

      val streamFiber =
        IO.both(messageStream, schedulerStream).flatMap { case (a, b) =>
          (a.mergeHaltBoth(b))
            .mergeHaltBoth(stopStream)
            .onFinalize(IO(scribe.debug(s"Stream terminated of $address")))
            .compile
            .drain
            .start.flatTap(_ => IO(scribe.debug(s"Streams of actor $address started.")))
        }

      Resource
        .make(streamFiber)(fiber =>
          fiber.cancel *> IO(
            scribe.debug(s"Streams of actor $address canceled.")
          )
        )
        .map { _ =>
          ()
        }
    }
  }

}
