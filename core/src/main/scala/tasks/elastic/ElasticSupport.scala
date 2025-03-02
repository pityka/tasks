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

trait ElasticSupport {

  def hostConfig: Option[HostConfiguration]

  def selfShutdownNow(): Unit

  trait Inner {
    def createRegistry: Option[NodeRegistry]
    def createSelfShutdown: Resource[IO, Unit]
    def getNodeName: String
  }
  def getNodeName: GetNodeName

  def apply(
      masterAddress: SimpleSocketAddress,
      queueActor: QueueActor,
      resource: ResourceAvailable,
      codeAddress: Option[CodeAddress],
      messenger: Messenger
  )(implicit config: TasksConfig): Inner

}


case class SimpleElasticSupport(
    val hostConfig: Option[HostConfiguration],
    shutdown: ShutdownNode,
    createNodeFactory: CreateNodeFactory,
    val getNodeName: GetNodeName
) extends ElasticSupport { self =>

  def selfShutdownNow() =
    shutdown.shutdownRunningNode(RunningJobId(getNodeName.getNodeName))

  def apply(
      masterAddress: SimpleSocketAddress,
      queueActor: QueueActor,
      resource: ResourceAvailable,
      codeAddress: Option[CodeAddress],
      messenger: Messenger
  )(implicit config: TasksConfig) =
    new Inner {
      def getNodeName = self.getNodeName.getNodeName
      def createRegistry =
        codeAddress.map(codeAddress =>
          new NodeRegistry(
            unmanagedResource = resource,
            createNode = createNodeFactory.apply(masterAddress, codeAddress),
            decideNewNode = new SimpleDecideNewNode(codeAddress.codeVersion),
            shutdownNode = shutdown,
            targetQueue = queueActor,
            messenger = messenger
          )
        )
      def createSelfShutdown = SelfShutdown
        .make(
          shutdownRunningNode = shutdown,
          id = RunningJobId(this.getNodeName),
          queueActor = queueActor,
          messenger = messenger
        )
        .map(_ => ())
    }

}

case class ElasticSupportFqcn(fqcn: String)
