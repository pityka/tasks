/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Modified work, Copyright (c) 2016 Istvan Bartha

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

package tasks.util

import akka.actor.{Actor, PoisonPill, ActorRef, Cancellable, Props}
import akka.remote.DeadlineFailureDetector
import akka.remote.FailureDetector.Clock
import akka.remote.DisassociatedEvent
import akka.pattern.ask

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit.{SECONDS}
import java.util.concurrent.{TimeUnit, ScheduledFuture}

import tasks.util.eq._
import tasks.util._
import tasks.queue._

@SerialVersionUID(1L)
private[tasks] case object Ping extends Serializable

@SerialVersionUID(1L)
private[tasks] case object Pong extends Serializable

@SerialVersionUID(1L)
private[tasks] case class HeartBeatStopped(ac: ActorRef) extends Serializable

class HeartBeatActor(target: ActorRef)
    extends Actor
    with akka.actor.ActorLogging {

  @SerialVersionUID(1L)
  case object CheckHeartBeat

  private var scheduledHeartBeats: Cancellable = null

  private var failureDetector = new DeadlineFailureDetector(
      config.global.acceptableHeartbeatPause)

  override def preStart {
    log.debug(
        "HeartBeatActor start for: " + target + " " + failureDetector.acceptableHeartbeatPause)

    import context.dispatcher

    scheduledHeartBeats = context.system.scheduler.schedule(
        initialDelay = 0 seconds,
        interval = config.global.launcherActorHeartBeatInterval,
        receiver = self,
        message = CheckHeartBeat
    )

  }

  override def postStop {
    scheduledHeartBeats.cancel
    log.info("HeartBeatActor stopped.")
  }

  private def targetDown {
    context.system.eventStream.publish(HeartBeatStopped(target))
    self ! PoisonPill
  }

  def receive = {
    case DisassociatedEvent(localAddress, remoteAddress, inbound)
        if remoteAddress === target.path.address =>
      log.warning("DisassociatedEvent received. TargetDown.")
      targetDown

    case CheckHeartBeat =>
      if (!failureDetector.isAvailable) {
        targetDown
      } else {
        target ! Ping
      }

    case Pong | true =>
      failureDetector.heartbeat

  }

}
