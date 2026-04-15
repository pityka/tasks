package tasks.util
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import fs2.concurrent.Channel
import tasks.util.message._
private[tasks] object LocalMessenger {
  def make = Resource.make(
    Ref
      .of[IO, Map[Address, Channel[IO, Message]]](Map.empty)
      .map(x => new LocalMessenger(x))
  )(localMessenger =>
    localMessenger.channels.get.flatMap { map =>
      IO.parSequenceN(1)(map.values.toList.map(_.close)).void
    }
  )
}
private[tasks] class LocalMessenger(
    private[util] val channels: Ref[IO, Map[Address, Channel[IO, Message]]]
) extends Messenger {

  def listeningAddress = None
  def subscribe(address: Address): IO[fs2.Stream[IO, Message]] = {
    for {
      ch <- Channel.unbounded[IO, Message]
      _ <- channels.modify { map =>
        map.get(address.withoutUri) match {
          case None =>
            (map.updated(address.withoutUri, ch), IO.unit)
          case Some(_) =>
            (
              map,
              IO.raiseError[Unit](
                new RuntimeException(s"Address $address already subscribed")
              )
            )
        }
      }.flatten
    } yield ch.stream
  }

  def submit(message: Message): IO[Unit] =
    channels.get.flatMap { channel =>
      channel.get(message.to.withoutUri) match {
        case None =>
          IO.delay {
            scribe.error(
              s"Delivery address not found. Message dropped. ${message.data} . Available channels: ${channel.keySet.toList.sortBy(_.toString)}",
              message
            )
          }
        case Some(channel) =>
          channel.send(message).map {
            case Right(value) =>
              value
            case Left(_) =>
              scribe.warn(
                s"Channel of receiver was closed. Message dropped. ${message.data}",
                message
              )
          }
      }
    }

}
