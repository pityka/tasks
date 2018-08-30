/*
 * The MIT License
 *
 * Copyright (c) 2018 Istvan Bartha
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

package tasks

import akka.actor.{Actor, ActorRef, Props, ActorSystem}
import java.net.InetSocketAddress
import akka.event.LoggingAdapter
import scala.util._

import tasks.elastic._
import tasks.shared._
import tasks.util.config._

import scala.concurrent.Future

object JvmElasticSupport {

  val taskSystems = scala.collection.mutable.Map[String, Future[TaskSystem]]()

  val nodesShutdown = scala.collection.mutable.ArrayBuffer[String]()
  val nodesSelfShutdown = scala.collection.mutable.ArrayBuffer[RunningJobId]()

  trait Shutdown extends ShutdownNode {
    import scala.concurrent.ExecutionContext.Implicits.global
    def log: LoggingAdapter

    def shutdownRunningNode(nodeName: RunningJobId): Unit = {
      nodesShutdown += nodeName.value
      taskSystems(nodeName.value).foreach(_.shutdown)
    }

    def shutdownPendingNode(nodeName: PendingJobId): Unit = {
      nodesShutdown += nodeName.value
      taskSystems(nodeName.value).foreach(_.shutdown)
    }

  }

  trait NodeRegistryImp extends Actor with JobRegistry {

    def masterAddress: InetSocketAddress
    def codeAddress: CodeAddress

    def requestOneNewJobFromJobScheduler(
        requestSize: tasks.shared.CPUMemoryRequest)
      : Try[Tuple2[PendingJobId, CPUMemoryAvailable]] = {

      val jobid =
        java.util.UUID.randomUUID.toString.replaceAllLiterally("-", "")

      log.info("Start new task system")

      val ts = Future { defaultTaskSystem(s"""
    hosts.master = "${masterAddress.getHostName}:${masterAddress.getPort}"
    hosts.app = false
    tasks.elastic.engine= "tasks.JvmElasticSupport.JvmGrid"
    jobid = $jobid
    tasks.akka.actorsystem.name = $jobid   
    tasks.addShutdownHook = false 
    tasks.fileservice.storageURI="${config.storageURI.toString}"
    """) }(scala.concurrent.ExecutionContext.Implicits.global)
      import scala.concurrent.ExecutionContext.Implicits.global
      ts.map(println).recover {
        case e =>
          println(e)
      }

      taskSystems += ((jobid, ts))

      Try(
        (PendingJobId(jobid),
         CPUMemoryAvailable(cpu = requestSize.cpu._1,
                            memory = requestSize.memory)))

    }

    def initializeNode(node: Node): Unit = {
      val ac = node.launcherActor

      context.actorOf(Props(new NodeKiller(ac, node))
                        .withDispatcher("my-pinned-dispatcher"),
                      "nodekiller" + node.name.value.replace("://", "___"))
    }

  }

  class NodeKiller(
      val targetLauncherActor: ActorRef,
      val targetNode: Node
  )(implicit val config: TasksConfig)
      extends NodeKillerImpl
      with Shutdown
      with akka.actor.ActorLogging

  class NodeRegistry(
      val masterAddress: InetSocketAddress,
      val targetQueue: ActorRef,
      override val unmanagedResource: CPUMemoryAvailable,
      val codeAddress: CodeAddress
  )(implicit val config: TasksConfig)
      extends NodeRegistryImp
      with NodeCreatorImpl
      with SimpleDecideNewNode
      with Shutdown
      with akka.actor.ActorLogging {
    def codeVersion = codeAddress.codeVersion
  }

  class TestSelfShutdown(val id: RunningJobId, val balancerActor: ActorRef)(
      implicit val config: TasksConfig)
      extends SelfShutdown {
    def shutdownRunningNode(nodeName: RunningJobId): Unit = {
      import scala.concurrent.ExecutionContext.Implicits.global

      nodesSelfShutdown += nodeName
      taskSystems(nodeName.value).foreach(_.shutdown)
    }
  }

  object JvmGrid extends ElasticSupport[NodeRegistry, SelfShutdown] {

    def fqcn = "tasks.JmvGrid"

    def hostConfig(implicit config: TasksConfig) = None

    def reaper(implicit config: TasksConfig, system: ActorSystem) = None

    def apply(masterAddress: InetSocketAddress,
              balancerActor: ActorRef,
              resource: CPUMemoryAvailable,
              codeAddress: Option[CodeAddress])(implicit config: TasksConfig) =
      new Inner {
        def getNodeName = config.raw.getString("jobid")
        def createRegistry =
          codeAddress.map(
            codeAddress =>
              new NodeRegistry(masterAddress,
                               balancerActor,
                               resource,
                               codeAddress))
        def createSelfShutdown =
          new TestSelfShutdown(RunningJobId(getNodeName), balancerActor)
      }

    override def toString = "jvmtest"

  }

}
