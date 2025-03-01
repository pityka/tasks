package tasks.util

import tasks.util.config.TasksConfig
import scala.concurrent.duration.FiniteDuration
import cats.effect.kernel.Ref
import cats.effect.IO
import cats.effect.kernel.Clock
import cats.effect.kernel.Resource
import tasks.util.message._
private[tasks] object Ask {

  def ask(
      target: Address,
      data: MessageData,
      timeout: FiniteDuration,
      messenger: Messenger
  ): IO[Either[Throwable, Option[Message]]] = {

    val address: Address = Address(
      s"Ask-target=$target-${data.hashCode}-${scala.util.Random.nextString(64)}"
    )
    val subscriber = messenger.subscribe(address)
    val message = Message(from = address, to = target, data = data)

    IO {
      scribe.debug(s"Ask start for $target")
    }.flatMap { _ =>
      subscriber.flatMap { messageStream =>
        messenger
          .submit(message)
          .flatMap { _ =>
            messageStream.take(1).compile.last.timeout(timeout).attempt

          }
      }
    }

  }
}
