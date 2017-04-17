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

package tasks.elastic.ssh

import akka.actor.{Actor, PoisonPill, ActorRef, Props, Cancellable}
import scala.concurrent.duration._
import java.util.concurrent.{TimeUnit, ScheduledFuture}
import java.net.InetSocketAddress
import akka.actor.Actor._
import akka.event.LoggingAdapter
import scala.util._

import collection.JavaConversions._
import java.io.File
import com.typesafe.config.{Config, ConfigObject}

import tasks.elastic._
import tasks.deploy._
import tasks.shared._
import tasks.shared.monitor._
import tasks.util._
import tasks.queue._

object SSHSettings {

  case class Host(hostname: String,
                  keyFile: File,
                  username: String,
                  memory: Int,
                  cpu: Int,
                  extraArgs: String)
  object Host {
    def fromConfig(config: Config) = {
      val hostname = config.getString("hostname")
      val keyFile = new File(config.getString("keyFile"))
      val username = config.getString("username")
      val memory = config.getInt("memory")
      val cpu = config.getInt("cpu")
      val extraArgs = Try(config.getString("extraArgs")).toOption.getOrElse("")
      Host(hostname, keyFile, username, memory, cpu, extraArgs)
    }
  }

  val hosts: collection.mutable.Map[String, (Host, Boolean)] =
    collection.mutable.Map(config.global.sshHosts.map {
      case (key, value) =>
        val host = Host.fromConfig(value.asInstanceOf[ConfigObject].toConfig)
        (host.hostname, (host, true))
    }.toList: _*)

  def disableHost(h: String) = synchronized {
    if (hosts.contains(h)) {
      hosts.update(h, (hosts(h)._1, false))
    }
  }

  def enableHost(h: String) = synchronized {
    if (hosts.contains(h)) {
      hosts.update(h, (hosts(h)._1, true))
    }
  }

}

object SSHOperations {
  import ch.ethz.ssh2.{Connection, KnownHosts, ServerHostKeyVerifier, Session}

  def openSession[T](host: SSHSettings.Host)(f: Session => T): Try[T] = {
    val connection = new Connection(host.hostname);

    val r = Try {
      connection.connect(HostKeyVerifier)
      connection.authenticateWithPublicKey(host.username, host.keyFile, null)
      val session = connection.openSession()
      val r = f(session)
      session.close
      r
    }

    connection.close

    r
  }

  object HostKeyVerifier extends ServerHostKeyVerifier {
    val kh = new KnownHosts(
        new File(System.getProperty("user.home") + "/.ssh/known_hosts"))
    def verifyServerHostKey(hostname: String,
                            port: Int,
                            serverHostKeyAlgorithm: String,
                            serverHostKey: Array[Byte]) =
      kh.verifyHostkey(hostname, serverHostKeyAlgorithm, serverHostKey) == KnownHosts.HOSTKEY_IS_OK
  }

  def terminateProcess(host: SSHSettings.Host, pid: String): Unit = {
    openSession(host) { session =>
      session.execCommand(s"kill $pid")
    }
  }

}

trait SSHShutdown extends ShutdownNode {

  def log: LoggingAdapter

  def shutdownRunningNode(nodeName: RunningJobId): Unit = {
    val hostname = nodeName.value.split(":")(0)
    val pid = nodeName.value.split(":")(1)
    SSHOperations.terminateProcess(SSHSettings.hosts(hostname)._1, pid)
    SSHSettings.enableHost(hostname)
  }

  def shutdownPendingNode(nodeName: PendingJobId): Unit = ()

}

trait SSHNodeRegistryImp extends Actor with GridJobRegistry {

  val masterAddress: InetSocketAddress

  def requestOneNewJobFromGridScheduler(requestSize: CPUMemoryRequest)
    : Try[Tuple2[PendingJobId, CPUMemoryAvailable]] =
    SSHSettings.hosts
      .filter(x => x._2._2 == true)
      .filter(x =>
            x._2._1.cpu >= requestSize.cpu._1 && x._2._1.memory >= requestSize.memory)
      .iterator
      .map {
        case (name, (host, _)) =>
          val script = Deployment.script(
              memory = requestSize.memory,
              gridEngine = SSHGrid,
              masterAddress = masterAddress,
              download = new java.net.URL("http",
                                          masterAddress.getHostName,
                                          masterAddress.getPort + 1,
                                          "/")
          )
          // Try(
          SSHOperations.openSession(host) { session =>
            val command =
              "source .bash_profile; " + script + "echo $!;exit;"

            session.execCommand(command)

            session.getStdin.close

            session.waitForCondition(ch.ethz.ssh2.ChannelCondition.STDOUT_DATA,
                                     10000)

            val stdout =
              scala.io.Source.fromInputStream(session.getStdout).mkString

            val pid = stdout.trim.toInt

            SSHSettings.disableHost(name)

            (PendingJobId(host.hostname + ":" + pid.toString),
             CPUMemoryAvailable(cpu = host.cpu, memory = host.memory))

          }
      }
      .find(_.isSuccess)
      .getOrElse(Failure(new RuntimeException("No enabled/working hosts")))

  def initializeNode(node: Node): Unit = {
    val ac = node.launcherActor //.revive

    val ackil = context.actorOf(
        Props(new SSHNodeKiller(ac, node))
          .withDispatcher("my-pinned-dispatcher"),
        "nodekiller" + node.name.value.replace("://", "___"))
  }

}

class SSHNodeKiller(
    val targetLauncherActor: ActorRef,
    val targetNode: Node
) extends NodeKillerImpl
    with SSHShutdown
    with akka.actor.ActorLogging

class SSHNodeRegistry(
    val masterAddress: InetSocketAddress,
    val targetQueue: ActorRef,
    override val unmanagedResource: CPUMemoryAvailable
) extends SSHNodeRegistryImp
    with NodeCreatorImpl
    with SimpleDecideNewNode
    with SSHShutdown
    with akka.actor.ActorLogging

class SSHSelfShutdown(val id: RunningJobId, val balancerActor: ActorRef)
    extends SelfShutdown {
  def shutdownRunningNode(nodeName: RunningJobId): Unit = {
    System.exit(0)
  }
}

object SSHGrid extends ElasticSupport[SSHNodeRegistry, SSHSelfShutdown] {

  def apply(master: InetSocketAddress,
            balancerActor: ActorRef,
            resource: CPUMemoryAvailable) = new Inner {
    def getNodeName = {
      val pid = java.lang.management.ManagementFactory
        .getRuntimeMXBean()
        .getName()
        .split("@")
        .head
      val hostname = tasks.util.config.global.hostName
      hostname + ":" + pid
    }
    def createRegistry = new SSHNodeRegistry(master, balancerActor, resource)
    def createSelfShutdown =
      new SSHSelfShutdown(RunningJobId(getNodeName), balancerActor)
  }

  override def toString = "SSH"

}
