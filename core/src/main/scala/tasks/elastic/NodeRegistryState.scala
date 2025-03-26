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

import tasks.queue.{QueueActor}
import tasks.shared.monitor._
import tasks.shared._
import tasks.util.config._
import tasks.wire._
import scala.util.{Failure, Success}
import tasks.util.Messenger
import tasks.util.message.Message
import tasks.util.message.MessageData
import tasks.util.Actor
import cats.effect.unsafe.implicits.global
import tasks.util.message.Address
import tasks.util.message.Node
import cats.effect.IO
import cats.effect.kernel.Ref
import tasks.util.SimpleSocketAddress
import cats.effect.kernel.Resource
import tasks.util.Ask
import cats.effect.ExitCode
import cats.effect.kernel.Deferred

private[tasks] object NodeRegistryState {

  sealed trait Event
  case object AllStop extends Event 
  case object NodeRequested extends Event
  case class NodeIsPending(
      pendingJobId: PendingJobId,
      resource: ResourceAvailable
  ) extends Event
  case class NodeIsUp(node: Node, pendingJobId: PendingJobId) extends Event
  case class NodeIsDown(node: Node) extends Event
  case class InitFailed(pending: PendingJobId) extends Event

  case class State(
      running: Map[RunningJobId, ResourceAvailable],
      pending: Map[PendingJobId, ResourceAvailable],
      cumulativeRequested: Int
  ) {
    def update(e: Event): State = {
      e match {
        case AllStop => copy(running = Map.empty, pending = Map.empty)
        case NodeRequested =>
          copy(cumulativeRequested = cumulativeRequested + 1)
        case NodeIsUp(Node(runningJobId, resource, _), pendingJobId) =>
          copy(
            pending = pending - pendingJobId,
            running = running + ((runningJobId, resource))
          )
        case NodeIsDown(Node(runningJobId, _, _)) =>
          copy(running = running - runningJobId)
        case InitFailed(pendingJobId) => copy(pending = pending - pendingJobId)
        case NodeIsPending(pendingJobId, resource) =>
          copy(pending = pending + ((pendingJobId, resource)))
      }
    }
  }

  object State {
    val empty = State(Map(), Map(), 0)
  }

}
