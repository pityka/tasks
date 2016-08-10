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

package tasks.elastic.drmaa

import org.ggf.drmaa.{Session, JobTemplate, SessionFactory, DrmaaException}
import tasks.elastic.TaskAllocationConstants._
import java.net.InetSocketAddress
import akka.actor.Actor._
import akka.actor.{ActorRef, Actor, Props}
import java.net.InetSocketAddress
import scala.util._
import akka.actor.ActorLogging
import akka.event.LoggingAdapter

import tasks.elastic._
import tasks.shared._
import tasks.deploy._
import tasks.util._

object DRMAAConfig {
  // val NumberOfCoresOfNewLauncher = config.global.getInt("tasks.elastic.drmaa.newNodeSize")
  //
  // val NumberOfCoresPerNode = scala.util.Try(config.global.getInt("tasks.elastic.lsf.span")).toOption.getOrElse(NumbeNumberOfCoresOfNewLauncher)
  //
  // val RequestedMemOfNewNode = config.global.getInt("tasks.elastic.lsf.requestedMemOfNewNode")

  val EmailAddress = config.global.getString("tasks.elastic.drmaa.email")

}
import DRMAAConfig._

object DRMAA {

  def testDRMAAConnection {
    val factory = SessionFactory.getFactory();
    val session = factory.getSession();
    session.init("");
    session.exit();
  }

}

class DRMAANodeKiller(
    val targetLauncherActor: ActorRef,
    val targetNode: Node
) extends NodeKillerImpl
    with DRMAAShutdown
    with akka.actor.ActorLogging

class SGENodeRegistry(
    val masterAddress: InetSocketAddress,
    val targetQueue: ActorRef,
    override val unmanagedResource: CPUMemoryAvailable
) extends DRMAAJobRegistry
    with NodeCreatorImpl
    with SimpleDecideNewNode
    with DRMAAShutdown

trait DRMAAShutdown extends ShutdownNode {

  def log: LoggingAdapter

  def shutdownRunningNode(nodeName: RunningJobId) {
    val factory = SessionFactory.getFactory();
    val session = factory.getSession();
    try {

      session.init("");
      session.control(nodeName.value, Session.TERMINATE);

    } catch {
      case e: Exception =>
        log.warning(
            "Some error in drmaa (possible other session is running." + e)
    } finally {
      session.exit();
    }
  }

  def shutdownPendingNode(node: PendingJobId) =
    shutdownRunningNode(RunningJobId(node.value))

}

trait DRMAAJobRegistry
    extends akka.actor.Actor
    with GridJobRegistry
    with ActorLogging {

  val masterAddress: InetSocketAddress

  def requestOneNewJobFromGridScheduler(resourceRequest: CPUMemoryRequest)
    : Try[Tuple2[PendingJobId, CPUMemoryAvailable]] = {
    import scala.collection.JavaConversions._

    val NumberOfCoresOfNewLauncher = resourceRequest.cpu._1
    val RequestedMemOfNewNode = resourceRequest.memory

    log.debug("request new node start (send new job to gridengine).")

    val jarpath = NewNodeJarPath
    val mem = RequestedMemOfNewNode
    val email = EmailAddress

    val javaargs: List[String] = "-Dconfig.file=" + System.getProperty(
          "config.file") :: "-Dhosts.gridengine=SGE" ::
        AdditionalSystemProperties :::
          "-Dhosts.master=" + masterAddress.getHostName + ":" + masterAddress.getPort :: jarpath :: Nil

    val factory = SessionFactory.getFactory();
    val session = factory.getSession();

    val jobid = Try {
      session.init(null);
      val jt = session.createJobTemplate();
      jt.setRemoteCommand("/usr/bin/java");
      jt.setArgs(javaargs);
      jt.setEmail(Set(email))
      jt.setNativeSpecification(
          "-V -cwd -pe ptile " + NumberOfCoresOfNewLauncher.toString)

      val jobid = session.runJob(jt)

      session.deleteJobTemplate(jt);

      (
          PendingJobId(jobid),
          CPUMemoryAvailable(NumberOfCoresOfNewLauncher, RequestedMemOfNewNode)
      )
    }

    session.exit()

    log.debug("job submitted. jobid: " + jobid)

    jobid

  }

  def initializeNode(node: Node) {
    val ac = node.launcherActor //.revive

    val ackil = context.actorOf(Props(new DRMAANodeKiller(ac, node)))

  }

}

class SGESelfShutdown(val id: RunningJobId, val balancerActor: ActorRef)
    extends SelfShutdown
    with DRMAAShutdown

class SGEReaper(val id: RunningJobId) extends ShutdownReaper with DRMAAShutdown

// sge_hostname_var contains the current hostname
trait SGEHostConfiguration extends HostConfiguration {

  val sge_hostname_var: String

  private val myPort = chooseNetworkPort

  lazy val myAddress = new InetSocketAddress(sge_hostname_var, myPort)

}

// if hosts.master is not present, current is master.
object SGEMasterSlave
    extends MasterSlaveConfiguration
    with SGEHostConfiguration {

  val sge_hostname_var = (System.getProperty("hosts.hostname", "") match {
    case x if x == "" =>
      System.getenv("HOSTNAME") match {
        case x if x == null => config.global.getString("hosts.hostname")
        case x => x
      }
    case x => x
  })

  val myCardinality = (System.getProperty("hosts.numCPU", "") match {
    case x if x == "" =>
      System.getenv("NSLOTS") match {
        case x if x == null => config.global.getString("hosts.numCPU")
        case x => x
      }
    case x => x
  }).toInt

  val availableMemory = config.global.getInt("hosts.RAM")

}
