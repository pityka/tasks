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

    IO {
      scribe.trace(s"Ask start for $target")
      val address: Address = Address(
        s"Ask-target=$target-${data.hashCode}-${scala.util.Random.alphanumeric.take(256).mkString}"
      )
      val subscriber = messenger.subscribe(address)
      val message = Message(from = address, to = target, data = data)
      (message, subscriber)
    }.flatMap { case (message, subscriber) =>
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
