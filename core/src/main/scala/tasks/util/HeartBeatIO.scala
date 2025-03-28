package tasks.util

import tasks.util.config.TasksConfig
import scala.concurrent.duration.FiniteDuration
import cats.effect.kernel.Ref
import cats.effect.IO
import cats.effect.kernel.Clock
import cats.effect.kernel.Resource
import tasks.util.message._
private[tasks] object HeartBeatIO {

  case class Deadline(
      acceptableHeartbeatPause: FiniteDuration,
      last: Ref[IO, FiniteDuration]
  )(implicit clock: Clock[IO]) {

    def isAvailable(): IO[Boolean] = for {
      last <- last.get
      current <- clock.realTime
    } yield (current - last) < acceptableHeartbeatPause

    def heartbeat(): IO[Unit] = for {
      current <- clock.realTime
      _ <- last.set(current)
    } yield ()

  }
  object Deadline {
    def make(
        acceptableHeartbeatPause: FiniteDuration
    )(implicit clock: Clock[IO]): IO[Deadline] = for {
      current <- clock.realTime
      ref <- Ref.of[IO, FiniteDuration](current)
    } yield Deadline(acceptableHeartbeatPause, ref)
  }

  def makeNonActor(target: IO[Unit], sideEffect: IO[Unit])(implicit
      config: TasksConfig,
      clock: Clock[IO]
  ) = {

    val deadline = Deadline.make(config.acceptableHeartbeatPause * 2)

    deadline.flatMap { deadline =>
      val beat : IO[Unit] = target.flatMap { _ =>
        deadline.heartbeat()
      }

      val check: IO[Boolean] =
        deadline.isAvailable().flatMap { isAvailable =>
          if (!isAvailable) {
            IO(
              scribe.warn("Heartbeat deadline fail. Call side effect")
            ) *>
              sideEffect.map((_:Unit) => false)
          } else
            beat.start.map(_ => true)

        }

      fs2.Stream
        .fixedRate[IO](config.launcherActorHeartBeatInterval)
        .evalMap(_ => check)
        .takeWhile(identity)
        .compile
        .drain
        .flatMap { _ =>
          IO {
            scribe.info(s"HeartBeatIO stopped. Target was $target.")
          }
        }

    }

  }
  def make(target: Address, sideEffect: IO[Unit], messenger: Messenger)(implicit
      config: TasksConfig,
      clock: Clock[IO]
  ) = {

    IO {
      scribe.info(
        s"HeartbeatIO start for $target with deadline ${config.acceptableHeartbeatPause * 2}"
      )
    }.flatMap { _ =>
      val address: Address = Address(
        s"HeartBeatIO-target=$target-${scala.util.Random.nextString(64)}"
      )
      val subscriber = messenger.subscribe(address)
      val pingMsg =
        Message(from = address, to = target, data = MessageData.Ping)

      val deadline = Deadline.make(config.acceptableHeartbeatPause * 2)

      deadline.flatMap { deadline =>
        subscriber.flatMap { messageStream =>
          val responseStream = messageStream.evalMap {
            case Message(MessageData.Ping, _, _) =>
              deadline.heartbeat()
            case _ =>
              IO.unit
          }

          val pingStream = fs2.Stream
            .fixedRate[IO](config.launcherActorHeartBeatInterval)
            .evalMap { _ =>
              deadline.isAvailable().flatMap { isAvailable =>
                if (!isAvailable) {
                  IO(
                    scribe.warn("Heartbeat deadline fail. Call side effect")
                  ) *>
                    sideEffect.map(_ => false)
                } else
                  messenger
                    .submit(pingMsg)
                    .map(_ => true)
              }

            }
            .takeWhile(identity)

          responseStream
            .mergeHaltR(pingStream.map(_ => ()))
            .compile
            .drain
            .flatMap { _ =>
              IO {
                scribe.info(s"HeartBeatIO stopped. Target was $target.")
              }
            }
        }
      }
    }

  }
}
