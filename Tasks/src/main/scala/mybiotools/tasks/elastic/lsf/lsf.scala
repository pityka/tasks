/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Copyright (c) 2016 Istvan Bartha
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

package tasks.elastic.lsf

import tasks.elastic._
import tasks.deploy._
import tasks.shared._
import tasks.shared.monitor._
import tasks.util._
import tasks.queue._

import akka.event.LoggingAdapter
import akka.actor._
import java.net._

import scala.util._
import scala.concurrent.duration._

trait LSFShutdown extends ShutdownNode {

  def log: LoggingAdapter

  def shutdownRunningNode(nodeName: RunningJobId) {
    try {
      val command = "bkill " + nodeName.value.toInt.toString
      log.info("Issuing command: " + command)
      execGetStreamsAndCode(command, atMost = 30 seconds)
    } catch {
      case e: Exception => log.error(e, "Some error happened during bkill.")
    }
    ()
  }

  def shutdownPendingNode(n: PendingJobId) =
    shutdownRunningNode(RunningJobId(n.value))
}

trait LSFNodeRegistryImp extends Actor with GridJobRegistry {

  val masterAddress: InetSocketAddress

  def requestOneNewJobFromGridScheduler(resourceRequest: CPUMemoryRequest)
    : Try[Tuple2[PendingJobId, CPUMemoryAvailable]] = {

    val jarpath = config.global.newNodeJarPath

    // val NumberOfCoresOfNewLauncher = resourceRequest.cpu._1
    // val RequestedMemOfNewNode = resourceRequest.memory

    val command = scala.sys.process.Process(
        "bsub" :: "-n" :: config.global.numberOfCoresOfNewLauncher.toString
          :: "-M " + config.global.requestedMemOfNewNode * 1000
            :: """-R span[ptile=%d] """.format(
                config.global.numberOfCoresPerNode)
              :: "-R rusage[mem=%d]".format(
                  config.global.requestedMemOfNewNode) ::
                "-u" :: config.global.emailAddress.toString ::
                  "-q" :: config.global.queueName.toString :: Nil)

    val command2 = """java -Xmx%sM -Dhosts.RAM=%s -Dhosts.list="$LSB_HOSTS" %s -Dconfig.global.file=%s -Dhosts.master=%s -Dhosts.gridengine=LSF -XX:ParallelGCThreads=%s -XX:CICompilerCount=%s %s """
      .format(
          (config.global.requestedMemOfNewNode * config.global.jvmMaxHeapFactor).toInt,
          config.global.requestedMemOfNewNode,
          config.global.additionalSystemProperties.mkString(" "),
          System.getProperty("config.global.file"),
          masterAddress.getHostName + ":" + masterAddress.getPort,
          math.max(6, config.global.numberOfCoresOfNewLauncher).toString,
          math.max(6, config.global.numberOfCoresOfNewLauncher).toString,
          jarpath
      )

    val command2is =
      new java.io.ByteArrayInputStream(command2.getBytes("UTF-8"));
    val jobid = Try {

      val (stdout, stderr, success) =
        execGetStreamsAndCode(command #< command2is, atMost = 30 seconds)

      log.debug("Bsub says:" + stdout.mkString("\n") ++ stderr.mkString("\n"))
      if (success && stderr == Nil) {
        val id = "<\\d+>".r
          .findFirstIn(stdout.head)
          .get
          .tail
          .dropRight(1)
          .toInt
          .toString
        (PendingJobId(id),
         CPUMemoryAvailable(config.global.numberOfCoresOfNewLauncher,
                            config.global.requestedMemOfNewNode))
      } else
        throw new RuntimeException(
            "Error in bsub: " + stdout.mkString("\n") + "\n" + stderr.mkString(
                "\n"))

    }

    jobid
  }

  def initializeNode(node: Node) {
    val ac = node.launcherActor //.revive

    // I think these actors could be moved to a normal Dispatcher
    val ackil = context.actorOf(
        Props(new LSFNodeKiller(ac, node))
          .withDispatcher("my-pinned-dispatcher"),
        "nodekiller" + node.name.value.replace("://", "___"))

  }

}

class LSFNodeRegistry(
    val masterAddress: InetSocketAddress,
    val targetQueue: ActorRef,
    override val unmanagedResource: CPUMemoryAvailable
) extends LSFNodeRegistryImp
    with NodeCreatorImpl
    with SimpleDecideNewNode
    with LSFShutdown
    with akka.actor.ActorLogging

class LSFNodeKiller(
    val targetLauncherActor: ActorRef,
    val targetNode: Node
) extends NodeKillerImpl
    with LSFShutdown
    with akka.actor.ActorLogging

class LSFSelfShutdown(val id: RunningJobId, val balancerActor: ActorRef)
    extends SelfShutdown
    with LSFShutdown

class LSFReaper(val id: RunningJobId) extends ShutdownReaper with LSFShutdown

// lsb_host_var contains the hostname as many times as many cpu is allocated on that node. It should contain only 1 hostname
trait LSFHostConfiguration extends HostConfiguration {

  val lsb_hosts_var: String

  val reservedCPU: Int

  private lazy val hostsAndSlots: Seq[(String, Int)] = {
    val splitted = lsb_hosts_var.split("\\s+").toList
    val unordered = splitted.groupBy(x => x).map(x => x._1 -> x._2.size).toSeq
    unordered.sortBy(x => splitted.indexOf(x._1))
  }

  private lazy val hosts: Tuple2[String, Int] = hostsAndSlots.head

  private val myPort = chooseNetworkPort

  lazy val myAddress = new InetSocketAddress(hosts._1, myPort)

  lazy val myCardinality = math.max(0, hosts._2 - reservedCPU)

}

object LSFMasterSlave
    extends MasterSlaveConfiguration
    with LSFHostConfiguration {

  val lsb_hosts_var: String = System.getProperty("hosts.list", "") match {
    case x if x == "" => "localhost"
    case x => x
  }

  val reservedCPU: Int = config.global.hostReservedCPU

  // TODO ask LSF
  val availableMemory = config.global.hostRAM

}
