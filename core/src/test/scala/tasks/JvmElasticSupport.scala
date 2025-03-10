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

import scala.util._

import tasks.elastic._
import tasks.shared._
import tasks.util.config._
import tasks.util.SimpleSocketAddress

import scala.concurrent.Future
import cats.effect.IO
import tasks.deploy.HostConfiguration
import cats.effect.kernel.Resource

object JvmElasticSupport {

  val taskSystems =
    scala.collection.mutable
      .ArrayBuffer[
        (String, Future[((TaskSystemComponents, HostConfiguration), IO[Unit])])
      ]()

  val nodesShutdown = scala.collection.mutable.ArrayBuffer[String]()

  object Shutdown extends ShutdownNode {
    import scala.concurrent.ExecutionContext.Implicits.global

    def shutdownRunningNode(nodeName: RunningJobId): Unit = synchronized {
      nodesShutdown += nodeName.value
      taskSystems
        .filter(_._1 == nodeName.value)
        .foreach(_._2.foreach { case (_, release) =>
          import cats.effect.unsafe.implicits.global

          release.unsafeRunSync()
        })
    }

    def shutdownPendingNode(nodeName: PendingJobId): Unit = synchronized {
      nodesShutdown += nodeName.value
      taskSystems
        .filter(_._1 == nodeName.value)
        .foreach(_._2.foreach { case (_, release) =>
          import cats.effect.unsafe.implicits.global

          release.unsafeRunSync()
        })
    }

  }

  class JvmCreateNode(masterAddress: SimpleSocketAddress) extends CreateNode {

    def requestOneNewJobFromJobScheduler(
        requestSize: tasks.shared.ResourceRequest
    )(implicit config: TasksConfig): Try[(PendingJobId, ResourceAvailable)] = {
      val jobid =
        java.util.UUID.randomUUID.toString.replace("-", "")

      val ts = Future {
        import cats.effect.unsafe.implicits.global

        defaultTaskSystem(
          s"""
    
    hosts.master = "${masterAddress.getHostName}:${masterAddress.getPort}"
    hosts.app = false
    tasks.disableRemoting = false
    jobid = $jobid
    tasks.addShutdownHook = false 
    tasks.fileservice.storageURI="${config.storageURI.toString}"
    """,
          Resource.pure(None),
          JvmGrid.make.map(Some(_))
        ).allocated.unsafeRunSync()
      }(scala.concurrent.ExecutionContext.Implicits.global)
      import scala.concurrent.ExecutionContext.Implicits.global
      ts.map(_ => ()).recover { case e =>
        println(e)
      }
      synchronized {
        taskSystems += ((jobid, ts))
      }
      Try(
        (
          PendingJobId(jobid),
          ResourceAvailable(
            cpu = requestSize.cpu._1,
            memory = requestSize.memory,
            scratch = requestSize.scratch,
            gpu = 0 until requestSize.gpu toList,
            image = None
          )
        )
      )

    }

  }

  class JvmCreateNodeFactory extends CreateNodeFactory {
    def apply(master: SimpleSocketAddress, codeAddress: CodeAddress) =
      new JvmCreateNode(master)
  }

  object JvmGetNodeName extends GetNodeName {
    def getNodeName = synchronized { taskSystems.last._1 }
  }

  object JvmGrid {

    def make: Resource[IO, ElasticSupport] = cats.effect.Resource.pure(
      SimpleElasticSupport(
        hostConfig = None,
        shutdown = Shutdown,
        createNodeFactory = new JvmCreateNodeFactory,
        getNodeName = JvmGetNodeName
      )
    )
  }
}
