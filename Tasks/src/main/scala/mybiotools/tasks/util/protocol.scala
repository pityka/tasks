package tasks.util

import akka.actor.ActorRef
import com.typesafe.config.Config
import akka.event.Logging.LogEvent

@SerialVersionUID(1L)
case class Introduction(
    queueActor: ActorRef,
    fileActor: ActorRef,
    nodeRegistry: Option[ActorRef],
    configuration: Config
) extends Serializable

@SerialVersionUID(1L)
case class ListenerEnvelope(ac: ActorRef, i: Int) extends Serializable

@SerialVersionUID(1L)
case class WaitingForShutdownCommand(i: Int) extends Serializable

@SerialVersionUID(1L)
case object ShutdownCommand extends Serializable

@SerialVersionUID(1L)
case object GetListener extends Serializable

@SerialVersionUID(1L)
case class ShutdownSystem(i: Int) extends Serializable
