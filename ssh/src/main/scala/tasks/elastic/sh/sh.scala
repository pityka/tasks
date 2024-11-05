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

import scala.util._
import scala.sys.process._

import tasks.elastic._
import tasks.shared._
import tasks.util._
import tasks.util.config._

object SHShutdown extends ShutdownNode {

  def shutdownRunningNode(nodeName: RunningJobId): Unit = {
    s"kill ${nodeName.value}".!
  }

  def shutdownPendingNode(nodeName: PendingJobId): Unit = ()

}

class SHCreateNode(
    masterAddress: SimpleSocketAddress,
    codeAddress: CodeAddress
)(implicit
    config: TasksConfig,
    elasticSupport: ElasticSupportFqcn
) extends CreateNode {

  def requestOneNewJobFromJobScheduler(
      requestSize: ResourceRequest
  ): Try[Tuple2[PendingJobId, ResourceAvailable]] = {
    val script = Deployment.script(
      memory = requestSize.memory,
      cpu = requestSize.cpu._2,
      scratch = requestSize.scratch,
      gpus = 0 until requestSize.gpu toList,
      elasticSupport = elasticSupport,
      masterAddress = masterAddress,
      download = Uri(
        scheme = "http",
        hostname = codeAddress.address.getHostName,
        port = codeAddress.address.getPort,
        path = "/"
      ),
      followerHostname = None,
      background = true,
      image = None
    )

    val wd = new java.io.File(config.shWorkDir)

    val (stdout, _, _) = execGetStreamsAndCode(
      Process(
        Seq(
          "bash",
          "-c",
          s"cd ${wd.getCanonicalPath};" + script
        )
      )
    )

    val pid = stdout.mkString("").trim.toInt

    Try(
      (
        PendingJobId(pid.toString),
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

class SHCreateNodeFactory(implicit
    config: TasksConfig,
    fqcn: ElasticSupportFqcn
) extends CreateNodeFactory {
  def apply(master: SimpleSocketAddress, codeAddress: CodeAddress) =
    new SHCreateNode(master, codeAddress)
}

object SHGetNodeName extends GetNodeName {
  def getNodeName = {
    val pid = java.lang.management.ManagementFactory
      .getRuntimeMXBean()
      .getName()
      .split("@")
      .head
    pid
  }
}

class SHElasticSupport extends ElasticSupportFromConfig {
  implicit val fqcn: ElasticSupportFqcn = ElasticSupportFqcn(
    "tasks.elastic.sh.SHElasticSupport"
  )
  def apply(implicit config: TasksConfig) = cats.effect.Resource.pure(SimpleElasticSupport(
    fqcn = fqcn,
    hostConfig = None,
    reaperFactory = None,
    shutdown = SHShutdown,
    createNodeFactory = new SHCreateNodeFactory,
    getNodeName = SHGetNodeName
  ))
}
