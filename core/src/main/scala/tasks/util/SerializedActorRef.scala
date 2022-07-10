package tasks.util

import akka.actor.ActorSystem
import scala.concurrent.duration.FiniteDuration
import akka.actor.ActorRef
import akka.serialization.Serialization

/** Utility class facilitating sending ActorRefs over the wire
  *
  * This class is not used internally in tasks, it is to be used by client code
  *
  * The use case of this class is to set up real time communication between
  * tasks
  *
  * Serializers are provided for message transfer but this class is not meant
  * for persistent storage.
  */
case class SerializedActorRef private[tasks] (s: String) {
  def resolve(timeout: FiniteDuration)(implicit as: ActorSystem) = {
    as.actorSelection(s).resolveOne(timeout)
  }
}

object SerializedActorRef {
  def apply(actor: ActorRef): SerializedActorRef = SerializedActorRef(
    Serialization.serializedActorPath(actor)
  )
}
