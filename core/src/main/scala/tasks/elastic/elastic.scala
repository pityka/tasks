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
import cats.effect.kernel.Resource
import tasks.deploy.HostConfiguration
import cats.effect.IO
import tasks.queue.QueueActor
import cats.effect.kernel.Deferred
import cats.effect.ExitCode



trait GetNodeName {
  def getNodeName(config: TasksConfig): IO[RunningJobId]
}

trait ShutdownSelfNode {
  def shutdownRunningNode(
      exitCode: Deferred[IO, ExitCode],
      nodeName: RunningJobId
  ): IO[Unit]
}
trait ShutdownRunningNode {
  def shutdownRunningNode(nodeName: RunningJobId): IO[Unit]
}

trait ShutdownNode extends ShutdownRunningNode {
  def shutdownPendingNode(nodeName: PendingJobId): IO[Unit]
}

trait CreateNode {
  def requestOneNewJobFromJobScheduler(
      k: ResourceRequest
  )(implicit
      taskConfig: TasksConfig
  ): IO[Either[String, (PendingJobId, ResourceAvailable)]]

  def convertRunningToPending(p: RunningJobId): IO[Option[PendingJobId]] =
    IO.pure(Some(PendingJobId(p.value)))

  def initializeNode(node: Node): IO[Unit] = {
    val _ = node // suppress warning
    IO.unit
  }
}

trait CreateNodeFactory {
  def apply(
      masterAddress: SimpleSocketAddress,
      masterPrefix: String,
      codeAddress: CodeAddress
  ): CreateNode
}

private[tasks] trait DecideNewNode {
  def needNewNode(
      q: QueueStat,
      registeredNodes: Seq[ResourceAvailable],
      pendingNodes: Seq[ResourceAvailable]
  ): Map[ResourceRequest, Int]
}

case class CodeAddress(address: SimpleSocketAddress, codeVersion: CodeVersion)
