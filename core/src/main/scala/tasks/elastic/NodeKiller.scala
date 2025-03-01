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

import scala.concurrent.duration._
import tasks.util.message._
import tasks.util._
import tasks.util.config._
import tasks.wire._
import tasks.queue.LauncherActor
import cats.effect.unsafe.implicits.global
import cats.effect.IO
import cats.effect.FiberIO
import cats.effect.kernel.Ref

object NodeKiller {
  case class State(
      lastIdleSessionStart: Long = System.nanoTime(),
      lastIdleState: Long = 0L,
      targetIsIdle: Boolean = true,
      fibers: List[FiberIO[Unit]] = Nil
  )
}

class NodeKiller(
    shutdownNode: ShutdownNode,
    targetLauncherActor: LauncherActor,
    targetNode: Node,
    listener: Address,
    messenger: Messenger
)(implicit config: TasksConfig)
    extends Actor.ActorBehavior[NodeKiller.State, Unit](messenger) {
  val init = NodeKiller.State()
  def derive(ref: Ref[IO, NodeKiller.State]): Unit = ()
  val address = Address(s"NodeKiller-target=$targetLauncherActor")

  override def schedulers(
      ref: Ref[IO, NodeKiller.State]
  ): Option[IO[fs2.Stream[IO, Unit]]] = Some {
    HeartBeatIO
      .make(targetLauncherActor.address, IO.delay(shutdown()), messenger)
      .start
      .flatMap { fiber =>
        ref.update(st => st.copy(fibers = fiber :: st.fibers))
      }
      .map(_ =>
        fs2.Stream.fixedRate[IO](config.nodeKillerMonitorInterval).evalMap {
          _ =>
            ref.get
              .flatMap {
                state =>
                  if (
                    state.targetIsIdle &&
                    (System
                      .nanoTime() - state.lastIdleSessionStart) >= config.idleNodeTimeout.toNanos
                  ) {

                    IO(
                      scribe.info(
                        "Target is idle. Start shutdown sequence. Send PrepareForShutdown to " + targetLauncherActor
                      )
                    ) *> messenger
                      .submit(
                        Message(
                          MessageData.PrepareForShutdown,
                          from = address,
                          to = targetLauncherActor.address
                        )
                      )
                      .map(_ =>
                        scribe.info(
                          "PrepareForShutdown sent to " + targetLauncherActor
                        )
                      )

                  } else {
                    messenger
                      .submit(
                        Message(
                          MessageData.WhatAreYouDoing,
                          from = address,
                          to = targetLauncherActor.address
                        )
                      )
                  }
              }
        }
      )
  }

  override def release(st: NodeKiller.State): IO[Unit] =
    IO.parSequenceN(1)(st.fibers.map(_.cancel)).void

  def shutdown() = {
    scribe.info(
      "Shutting down target node: name= " + targetNode.name + " , actor= " + targetLauncherActor
    )
    shutdownNode.shutdownRunningNode(targetNode.name)
    sendTo(
      listener,
      MessageData.RemoveNode(targetNode)
    ) *> stopProcessingMessages
  }

  def receive = (state, ref) => {
    case Message(MessageData.Idling(idlingState), from, _) =>
      val st0 = if (state.lastIdleState < idlingState) {
        state.copy(
          lastIdleSessionStart = System.nanoTime(),
          lastIdleState = idlingState
        )
      } else state
      val st1 = st0.copy(targetIsIdle = true)
      (st1 -> IO.unit)

    case Message(MessageData.Working, from, _) =>
      state.copy(targetIsIdle = false) -> IO.unit

    case Message(MessageData.ReadyForShutdown, from, _) => state -> shutdown()
  }

}
