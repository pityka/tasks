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

import tasks.shared._
import tasks.util._
import tasks.util.config.TasksConfig
import tasks.wire._
import scala.util.Try
import tasks.util.message._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import tasks.queue.LauncherActor

case class Node(
    name: RunningJobId,
    size: ResourceAvailable,
    launcherActor: LauncherActor
)

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
  )(implicit taskConfig: TasksConfig): Try[(PendingJobId, ResourceAvailable)]

  def convertRunningToPending(p: RunningJobId): Option[PendingJobId] =
    Some(PendingJobId(p.value))

  def initializeNode(node: Node): Unit = {
    val _ = node // suppress warning
    ()
  }
}

trait CreateNodeFactory {
  def apply(
      masterAddress: SimpleSocketAddress,
      codeAddress: CodeAddress
  ): CreateNode
}

private[tasks] trait DecideNewNode {
  def needNewNode(
      q: MessageData.QueueStat,
      registeredNodes: Seq[ResourceAvailable],
      pendingNodes: Seq[ResourceAvailable]
  ): Map[ResourceRequest, Int]
}

case class CodeAddress(address: SimpleSocketAddress, codeVersion: CodeVersion)
