/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
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

package tasks.elastic

import akka.actor.{Actor, ActorRef, ActorLogging, PoisonPill, Cancellable}
import scala.concurrent.duration._

import tasks.util._
import tasks.util.config._
import tasks.wire._
import tasks.queue.LauncherActor

class NodeKiller(
    shutdownNode: ShutdownNode,
    targetLauncherActor: LauncherActor,
    targetNode: Node,
    listener: ActorRef
)(implicit config: TasksConfig)
    extends Actor
    with ActorLogging {

  private case object TargetStopped

  private var scheduler: Cancellable = null
  private var heartBeat: ActorRef = null

  override def preStart(): Unit = {
    log.debug(
      "NodeKiller start. Monitoring actor: " + targetLauncherActor + " on node: " + targetNode.name
    )

    import context.dispatcher

    scheduler = context.system.scheduler.scheduleAtFixedRate(
      initialDelay = 0 seconds,
      interval = config.nodeKillerMonitorInterval,
      receiver = self,
      message = MeasureTime
    )

    heartBeat =
      HeartBeatActor.watch(targetLauncherActor.actor, TargetStopped, self)

  }

  override def postStop(): Unit = {
    if (scheduler != null) {
      scheduler.cancel()
    }

    if (heartBeat != null) {
      heartBeat ! PoisonPill
    }

    log.info("NodeKiller stopped.")
  }

  var lastIdleSessionStart: Long = System.nanoTime()

  var lastIdleState: Long = 0L

  var targetIsIdle = true

  def shutdown() = {
    log.info(
      "Shutting down target node: name= " + targetNode.name + " , actor= " + targetLauncherActor
    )
    shutdownNode.shutdownRunningNode(targetNode.name)
    listener ! RemoveNode(targetNode)
    scheduler.cancel()
    self ! PoisonPill
  }

  def receive = {
    case TargetStopped =>
      shutdown()
    case MeasureTime =>
      if (targetIsIdle &&
          (System
            .nanoTime() - lastIdleSessionStart) >= config.idleNodeTimeout.toNanos) {
        try {
          log.info(
            "Target is idle. Start shutdown sequence. Send PrepareForShutdown to " + targetLauncherActor
          )
          targetLauncherActor.actor ! PrepareForShutdown
          log.info("PrepareForShutdown sent to " + targetLauncherActor)
        } catch {
          case _: java.nio.channels.ClosedChannelException => shutdown()
        }
      } else {
        targetLauncherActor.actor ! WhatAreYouDoing
      }

    case Idling(state) =>
      if (lastIdleState < state) {
        lastIdleSessionStart = System.nanoTime()
        lastIdleState = state
      }
      targetIsIdle = true

    case Working =>
      targetIsIdle = false

    case ReadyForShutdown => shutdown()
  }

}
