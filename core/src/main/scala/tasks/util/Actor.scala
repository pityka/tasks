package tasks.util

import cats.effect.kernel.Ref
import cats.effect.IO
import cats.effect.FiberIO
import cats.effect.kernel.Resource
import cats.effect.std.Queue

object Actor {

  abstract class ActorBehavior[State, K](messenger: Messenger) {
    type ReceiveIO =
      (State, Ref[IO, State]) => PartialFunction[Message, (State, IO[Unit])]
    def receive: ReceiveIO
    def release(st: State): IO[Unit] = IO.unit

    def sendTo(target: Address, msg: MessageData) =
      messenger.submit(Message(msg, from = address, to = target))
    def schedulers(ref: Ref[IO, State]): Option[IO[fs2.Stream[IO, Unit]]] = None
    val init: State
    val address: Address
    def derive(ref: Ref[IO, State]): K

    val stopQueue = Queue.bounded[IO, Option[Unit]](1)

    val stopProcessingMessages = stopQueue.flatMap(_.offer(None))

  }

  def makeFromBehavior[S, K](
      b: ActorBehavior[S, K],
      messenger: Messenger
  ): Resource[IO, K] = {
    val i = b.init
    val a = b.address
    make(
      init = i,
      address = a,
      schedulers = b.schedulers _,
      receive = b.receive,
      release = b.release _,
      messenger = messenger,
      stopQueue = b.stopQueue
    ).map { case stateRef => b.derive(stateRef) }
  }

  def make[State](
      init: State,
      stopQueue: IO[Queue[IO, Option[Unit]]],
      schedulers: Ref[IO, State] => Option[IO[fs2.Stream[IO, Unit]]],
      receive: (
          State,
          Ref[IO, State]
      ) => PartialFunction[Message, (State, IO[Unit])],
      release: State => IO[Unit],
      address: Address,
      messenger: Messenger
  ): Resource[IO, Ref[IO, State]] = {
    Resource.eval(Ref.of[IO, State](init)).flatMap { stateRef =>
      Resource.eval(stopQueue).flatMap { stopQueue =>
        val messageStream = messenger.subscribe(address).map { stream =>
          stream
            .evalMap { message =>
              stateRef.flatModify { state =>
                val (newState, sideEffect) = receive(state, stateRef)
                  .lift(message)
                  .getOrElse((state, IO.unit))
                (newState, sideEffect)
              }
            }
        }
        val stopStream = fs2.Stream
          .fromQueueNoneTerminated[IO, Unit](stopQueue, 1)
          .map(_ => ())
        val schedulerStream =
          schedulers(stateRef).getOrElse(IO.pure(fs2.Stream.never[IO]))

        val streamFiber =
          IO.both(messageStream, schedulerStream).flatMap { case (a, b) =>
            (a.mergeHaltBoth(b)).mergeHaltBoth(stopStream).compile.drain.start
          }

        val releaseIO =
          IO(scribe.debug(s"Will run release side effect of actor with address $address")) *> stateRef.get.flatMap(release).void

        Resource.make(streamFiber)(fiber => releaseIO *> fiber.cancel *> IO(scribe.debug(s"Streams of actor $address canceled."))).map {
          _ => stateRef
        }
      }
    }
  }

}
