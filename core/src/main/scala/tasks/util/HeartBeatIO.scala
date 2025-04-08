package tasks.util

import tasks.util.config.TasksConfig
import scala.concurrent.duration.FiniteDuration
import cats.effect.kernel.Ref
import cats.effect.IO
import cats.effect.kernel.Clock
import cats.effect.kernel.Resource
import tasks.util.message._
private[tasks] object HeartBeatIO {

  object Counter {

    def sideEffectWhenTimeout(
        query: IO[Long],
        sideEffect: IO[Unit]
    )(implicit config: TasksConfig) = {

      query
        .flatMap { a =>
          IO.sleep(config.launcherActorHeartBeatInterval * 2)
            .flatMap(_ => query)
            .map { b =>
              a == b
            }
        }
        .flatMap { noChange =>
          if (noChange) sideEffect
          else IO.unit *> IO("OKOK")
        }

    }
  }

}
