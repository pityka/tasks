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

import java.net.InetSocketAddress
import scala.util._

import tasks.elastic._
import tasks.shared._
import tasks.util.config._

import scala.concurrent.Future

object JvmElasticSupport {

  val taskSystems = scala.collection.mutable.Map[String, Future[TaskSystem]]()

  val nodesShutdown = scala.collection.mutable.ArrayBuffer[String]()

  object Shutdown extends ShutdownNode {
    import scala.concurrent.ExecutionContext.Implicits.global

    def shutdownRunningNode(nodeName: RunningJobId): Unit = {
      nodesShutdown += nodeName.value
      taskSystems.get(nodeName.value).foreach(_.foreach(_.shutdown))
    }

    def shutdownPendingNode(nodeName: PendingJobId): Unit = {
      nodesShutdown += nodeName.value
      taskSystems.get(nodeName.value).foreach(_.foreach(_.shutdown))
    }

  }

  class JvmCreateNode(masterAddress: InetSocketAddress)(
      implicit config: TasksConfig)
      extends CreateNode {

    def requestOneNewJobFromJobScheduler(
        requestSize: tasks.shared.CPUMemoryRequest)
      : Try[(PendingJobId, CPUMemoryAvailable)] = {
      val jobid =
        java.util.UUID.randomUUID.toString.replaceAllLiterally("-", "")

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

  }

  class JvmCreateNodeFactory(implicit config: TasksConfig)
      extends CreateNodeFactory {
    def apply(master: InetSocketAddress, codeAddress: CodeAddress) =
      new JvmCreateNode(master)
  }

  object JvmGetNodeName extends GetNodeName {
    def getNodeName = taskSystems.head._1
  }

  object JvmGrid extends ElasticSupportFromConfig {
    implicit val fqcn = ElasticSupportFqcn("tasks.JvmElasticSupport.JvmGrid")
    def apply(implicit config: TasksConfig) = SimpleElasticSupport(
      fqcn = fqcn,
      hostConfig = None,
      reaperFactory = None,
      shutdown = Shutdown,
      createNodeFactory = new JvmCreateNodeFactory,
      getNodeName = JvmGetNodeName
    )
  }
}
