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

import akka.actor.{ActorRef, ExtendedActorSystem, ActorSystem}
import java.net.InetSocketAddress

import tasks.shared.monitor._
import tasks.shared._
import tasks.util._
import tasks.util.config.TasksConfig
import tasks.wire._
import scala.util.Try

import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto._

case class Node(
    name: RunningJobId,
    size: ResourceAvailable,
    launcherActor: ActorRef
)

object Node {
  implicit val enc: Encoder[Node] = deriveEncoder[Node]
  implicit def dec(implicit as: ExtendedActorSystem): Decoder[Node] = {
    val _ = as // suppressing unused warning
    deriveDecoder[Node]
  }
}

trait GetNodeName {
  def getNodeName: String
}

trait ShutdownRunningNode {
  def shutdownRunningNode(nodeName: RunningJobId): Unit
}

trait ShutdownNode extends ShutdownRunningNode {
  def shutdownPendingNode(nodeName: PendingJobId): Unit
}

trait CreateNode {
  def requestOneNewJobFromJobScheduler(
      k: ResourceRequest
  ): Try[(PendingJobId, ResourceAvailable)]

  def convertRunningToPending(p: RunningJobId): Option[PendingJobId] =
    Some(PendingJobId(p.value))

  def initializeNode(node: Node): Unit = {
    val _ = node // suppress warning
    ()
  }
}

trait CreateNodeFactory {
  def apply(
      masterAddress: InetSocketAddress,
      codeAddres: CodeAddress
  ): CreateNode
}

trait ReaperFactory {
  def apply(implicit system: ActorSystem, config: TasksConfig): ActorRef
}

trait DecideNewNode {
  def needNewNode(
      q: QueueStat,
      registeredNodes: Seq[ResourceAvailable],
      pendingNodes: Seq[ResourceAvailable]
  ): Map[ResourceRequest, Int]
}

class ShutdownReaper(id: RunningJobId, shutdown: ShutdownRunningNode)
    extends Reaper {

  def allSoulsReaped(): Unit = {
    log.info(s"All souls reaped. Call shutdown node on $id.")
    shutdown.shutdownRunningNode(id)
  }
}

case class CodeAddress(address: InetSocketAddress, codeVersion: CodeVersion)
