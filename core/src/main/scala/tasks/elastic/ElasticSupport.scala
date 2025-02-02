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
import tasks.ui.EventListener
import tasks.util.SimpleSocketAddress

trait ElasticSupport {

  def fqcn: ElasticSupportFqcn

  def hostConfig: Option[HostConfiguration]

  def reaperFactory: Option[ReaperFactory]

  def selfShutdownNow(): Unit

  trait Inner {
    def createRegistry: Option[NodeRegistry]
    def createSelfShutdown: SelfShutdown
    def getNodeName: String
  }
  def getNodeName: GetNodeName

  def apply(
      masterAddress: SimpleSocketAddress,
      queueActor: QueueActor,
      resource: ResourceAvailable,
      codeAddress: Option[CodeAddress],
      eventListener: Option[EventListener[NodeRegistry.Event]]
  )(implicit config: TasksConfig): Inner

}

trait ElasticSupportFromConfig {

  def apply(implicit config: TasksConfig): cats.effect.Resource[cats.effect.IO,ElasticSupport]

}

case class SimpleElasticSupport(
    val fqcn: ElasticSupportFqcn,
    val hostConfig: Option[HostConfiguration],
    reaperFactory: Option[ReaperFactory],
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
      eventListener: Option[EventListener[NodeRegistry.Event]]
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
            eventListener = eventListener
          )
        )
      def createSelfShutdown =
        new SelfShutdown(
          shutdownRunningNode = shutdown,
          id = RunningJobId(this.getNodeName),
          queueActor = queueActor
        )
    }

}

case class ElasticSupportFqcn(fqcn: String)
