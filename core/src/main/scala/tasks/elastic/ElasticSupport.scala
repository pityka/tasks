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
import tasks.util.config._
import tasks.deploy._
import tasks.queue.QueueActor
import tasks.util.SimpleSocketAddress
import tasks.util.Messenger
import cats.effect.kernel.Resource
import cats.effect.IO
import cats.effect.kernel.Deferred
import cats.effect.ExitCode

trait ElasticSupport {
  def hostConfig: Option[TasksConfig => HostConfiguration]
  def shutdownFromNodeRegistry: ShutdownNode
  def shutdownFromWorker: ShutdownSelfNode
  def createNodeFactory: CreateNodeFactory
  def getNodeName: GetNodeName
  def convertRunningToPending: ConvertRunningToPending =
    ConvertRunningToPending.identity
  def needsPackageServer: Boolean = true
}

object ElasticSupport {
  private[tasks] final case class ElasticSupportImpl(
      hostConfig: Option[TasksConfig => HostConfiguration],
      shutdownFromNodeRegistry: ShutdownNode,
      shutdownFromWorker: ShutdownSelfNode,
      createNodeFactory: CreateNodeFactory,
      getNodeName: GetNodeName,
      override val convertRunningToPending: ConvertRunningToPending,
      override val needsPackageServer: Boolean
  ) extends ElasticSupport

  def apply(
      hostConfig: Option[TasksConfig => HostConfiguration],
      shutdownFromNodeRegistry: ShutdownNode,
      shutdownFromWorker: ShutdownSelfNode,
      createNodeFactory: CreateNodeFactory,
      getNodeName: GetNodeName,
      convertRunningToPending: ConvertRunningToPending,
      needsPackageServer: Boolean
  ): ElasticSupport =
    ElasticSupportImpl(
      hostConfig,
      shutdownFromNodeRegistry,
      shutdownFromWorker,
      createNodeFactory,
      getNodeName,
      convertRunningToPending,
      needsPackageServer
    )

  def apply(
      hostConfig: Option[TasksConfig => HostConfiguration],
      shutdownFromNodeRegistry: ShutdownNode,
      shutdownFromWorker: ShutdownSelfNode,
      createNodeFactory: CreateNodeFactory,
      getNodeName: GetNodeName,
      convertRunningToPending: ConvertRunningToPending
  ): ElasticSupport =
    apply(
      hostConfig,
      shutdownFromNodeRegistry,
      shutdownFromWorker,
      createNodeFactory,
      getNodeName,
      convertRunningToPending,
      true
    )

  def apply(
      hostConfig: Option[TasksConfig => HostConfiguration],
      shutdownFromNodeRegistry: ShutdownNode,
      shutdownFromWorker: ShutdownSelfNode,
      createNodeFactory: CreateNodeFactory,
      getNodeName: GetNodeName
  ): ElasticSupport =
    apply(
      hostConfig,
      shutdownFromNodeRegistry,
      shutdownFromWorker,
      createNodeFactory,
      getNodeName,
      ConvertRunningToPending.identity,
      true
    )
}
