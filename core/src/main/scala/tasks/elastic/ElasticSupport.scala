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

private[tasks] trait ElasticSupportInnerFactories {
  def registry: Option[NodeRegistry]
  def createSelfShutdown: Resource[IO, Unit]
  def getNodeName: IO[String]
}

final class ElasticSupport(
    val hostConfig: Option[HostConfiguration],
    shutdownFromNodeRegistry: ShutdownNode,
    shutdownFromWorker: ShutdownSelfNode,
    createNodeFactory: CreateNodeFactory,
    val getNodeName: GetNodeName
) { self =>

  def selfShutdownNow(
      exitCode: Deferred[IO, ExitCode],
      config: TasksConfig
  ): IO[Unit] =
    getNodeName
      .getNodeName(config)
      .flatMap(nodeName =>
        shutdownFromWorker.shutdownRunningNode(exitCode, RunningJobId(nodeName))
      )

  private[tasks] def apply(
      masterAddress: SimpleSocketAddress,
      masterPrefix: String,
      queueActor: QueueActor,
      resource: ResourceAvailable,
      codeAddress: Option[CodeAddress],
      messenger: Messenger,
      exitCode: Deferred[IO, ExitCode]
  )(implicit config: TasksConfig) =
    new ElasticSupportInnerFactories {
      def getNodeName = self.getNodeName.getNodeName(config)
      def registry =
        codeAddress.map(codeAddress =>
          new NodeRegistry(
            unmanagedResource = resource,
            createNode = createNodeFactory.apply(
              masterAddress = masterAddress,
              masterPrefix = masterPrefix,
              codeAddress = codeAddress
            ),
            decideNewNode = new SimpleDecideNewNode(codeAddress.codeVersion),
            shutdownNode = shutdownFromNodeRegistry,
            targetQueue = queueActor,
            messenger = messenger
          )
        )
      def createSelfShutdown =
        Resource.eval(this.getNodeName).flatMap { nodeName =>
          SelfShutdown
            .make(
              shutdownRunningNode = shutdownFromWorker,
              id = RunningJobId(nodeName),
              queueActor = queueActor,
              messenger = messenger,
              exitCode = exitCode
            )
            .map(_ => ())
        }
    }

}
