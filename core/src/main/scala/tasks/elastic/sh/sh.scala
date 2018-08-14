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

package tasks.elastic.sh

import akka.actor.{Actor, ActorRef, Props}
import java.net.InetSocketAddress
import akka.event.LoggingAdapter
import scala.util._
import scala.sys.process._

import tasks.elastic._
import tasks.shared._
import tasks.util._
import tasks.util.config._

trait SHShutdown extends ShutdownNode {

  def log: LoggingAdapter

  def shutdownRunningNode(nodeName: RunningJobId): Unit = {
    execGetStreamsAndCode(s"kill ${nodeName.value}")
  }

  def shutdownPendingNode(nodeName: PendingJobId): Unit = ()

}

trait SHNodeRegistryImp extends Actor with GridJobRegistry {

  def masterAddress: InetSocketAddress
  def codeAddress: CodeAddress

  def requestOneNewJobFromGridScheduler(requestSize: CPUMemoryRequest)
    : Try[Tuple2[PendingJobId, CPUMemoryAvailable]] = {
    val script = Deployment.script(
      memory = requestSize.memory,
      gridEngine = SHGrid,
      masterAddress = masterAddress,
      download = new java.net.URL("http",
                                  codeAddress.address.getHostName,
                                  codeAddress.address.getPort,
                                  "/")
    )

    val (stdout, _, _) = execGetStreamsAndCode(
      Process(Seq("bash", "-c", script + "echo $!;exit;")))

    val pid = stdout.mkString("").trim.toInt

    Try(
      (PendingJobId(pid.toString),
       CPUMemoryAvailable(cpu = requestSize.cpu._1,
                          memory = requestSize.memory)))

  }

  def initializeNode(node: Node): Unit = {
    val ac = node.launcherActor

    context.actorOf(Props(new SHNodeKiller(ac, node))
                      .withDispatcher("my-pinned-dispatcher"),
                    "nodekiller" + node.name.value.replace("://", "___"))
  }

}

class SHNodeKiller(
    val targetLauncherActor: ActorRef,
    val targetNode: Node
)(implicit val config: TasksConfig)
    extends NodeKillerImpl
    with SHShutdown
    with akka.actor.ActorLogging

class SHNodeRegistry(
    val masterAddress: InetSocketAddress,
    val targetQueue: ActorRef,
    override val unmanagedResource: CPUMemoryAvailable,
    val codeAddress: CodeAddress
)(implicit val config: TasksConfig)
    extends SHNodeRegistryImp
    with NodeCreatorImpl
    with SimpleDecideNewNode
    with SHShutdown
    with akka.actor.ActorLogging {
  def codeVersion = codeAddress.codeVersion
}

class SHSelfShutdown(val id: RunningJobId, val balancerActor: ActorRef)
    extends SelfShutdown {
  def shutdownRunningNode(nodeName: RunningJobId): Unit = {
    System.exit(0)
  }
}

object SHGrid extends ElasticSupport[SHNodeRegistry, SHSelfShutdown] {

  def apply(masterAddress: InetSocketAddress,
            balancerActor: ActorRef,
            resource: CPUMemoryAvailable,
            codeAddress: CodeAddress)(implicit config: TasksConfig) =
    new Inner {
      def getNodeName = {
        val pid = java.lang.management.ManagementFactory
          .getRuntimeMXBean()
          .getName()
          .split("@")
          .head
        pid
      }
      def createRegistry =
        new SHNodeRegistry(masterAddress, balancerActor, resource, codeAddress)
      def createSelfShutdown =
        new SHSelfShutdown(RunningJobId(getNodeName), balancerActor)
    }

  override def toString = "SH"

}
