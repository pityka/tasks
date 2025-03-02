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
import cats.effect.kernel.Resource
import cats.effect.IO

object SHShutdown extends ShutdownNode {

  def shutdownRunningNode(nodeName: RunningJobId): Unit = {
    s"kill ${nodeName.value}".!
  }

  def shutdownPendingNode(nodeName: PendingJobId): Unit = ()

}

class SHCreateNode(
    masterAddress: SimpleSocketAddress,
    codeAddress: CodeAddress
) extends CreateNode {

  /** Execute command with user function to process each line of output.
    *
    * Based on from
    * http://www.jroller.com/thebugslayer/entry/executing_external_system_commands_in
    * Creates 2 new threads: one for the stdout, one for the stderror.
    * @param pb
    *   Description of the executable process
    * @return
    *   Exit code of the process.
    */
  private def exec(
      pb: ProcessBuilder
  )(stdOutFunc: String => Unit = { (_: String) => })(implicit
      stdErrFunc: String => Unit = (_: String) => ()
  ): Int =
    pb.run(ProcessLogger(stdOutFunc, stdErrFunc)).exitValue()

  /** Execute command. Returns stdout and stderr as strings, and true if it was
    * successful.
    *
    * A process is considered successful if its exit code is 0 and the error
    * stream is empty. The latter criterion can be disabled with the
    * unsuccessfulOnErrorStream parameter.
    * @param pb
    *   The process description.
    * @param unsuccessfulOnErrorStream
    *   if true, then the process is considered as a failure if its stderr is
    *   not empty.
    * @param atMost
    *   max waiting time.
    * @return
    *   (stdout,stderr,success) triples
    */
  private def execGetStreamsAndCode(
      pb: ProcessBuilder,
      unsuccessfulOnErrorStream: Boolean = true
  ): (List[String], List[String], Boolean) = {
    var ls: List[String] = Nil
    var lse: List[String] = Nil
    var boolean = true
    val exitvalue = exec(pb) { ln =>
      ls = ln :: ls
    } { ln =>
      if (unsuccessfulOnErrorStream) {
        boolean = false
      }; lse = ln :: lse
    }
    (ls.reverse, lse.reverse, boolean && (exitvalue == 0))
  }

  def requestOneNewJobFromJobScheduler(
      requestSize: ResourceRequest
  )(implicit
      config: TasksConfig
  ): Try[Tuple2[PendingJobId, ResourceAvailable]] = {
    val script = Deployment.script(
      memory = requestSize.memory,
      cpu = requestSize.cpu._2,
      scratch = requestSize.scratch,
      gpus = 0 until requestSize.gpu toList,
      masterAddress = masterAddress,
      download = Uri(
        scheme = "http",
        hostname = codeAddress.address.getHostName,
        port = codeAddress.address.getPort,
        path = "/"
      ),
      followerHostname = None,
      followerExternalHostname = None,
      followerMayUseArbitraryPort = true,
      followerNodeName = None,
      background = true,
      image = None
    )

    val wd = new java.io.File(config.shWorkDir)

    val (stdout, _, _) = execGetStreamsAndCode(
      Process(
        Seq(
          "bash",
          "-c",
          script
        ),
        cwd = wd
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

class SHCreateNodeFactory extends CreateNodeFactory {
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

object SHElasticSupport {
  def make: Resource[IO, ElasticSupport] = cats.effect.Resource.pure(
    SimpleElasticSupport(
      hostConfig = None,
      shutdown = SHShutdown,
      createNodeFactory = new SHCreateNodeFactory,
      getNodeName = SHGetNodeName
    )
  )
}
