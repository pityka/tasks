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

import scala.util._

import scala.jdk.CollectionConverters._
import java.io.File
import com.typesafe.config.{Config, ConfigObject}

import tasks.elastic._
import tasks.shared._
import tasks.util.config._
import tasks.util.SimpleSocketAddress
import tasks.util.Uri

object SSHSettings {
  case class Host(
      hostname: String,
      keyFile: File,
      username: String,
      memory: Int,
      cpu: Int,
      scratch: Int,
      extraArgs: String,
      gpu: List[Int]
  )
  object Host {
    def fromConfig(config: Config) = {
      val hostname = config.getString("hostname")
      val keyFile = new File(config.getString("keyFile"))
      val username = config.getString("username")
      val memory = config.getInt("memory")
      val cpu = config.getInt("cpu")
      val scratch = config.getInt("scratch")
      val gpu =
        Try(config.getIntList("gpu").asScala.map(_.toInt).toList).toOption
          .getOrElse(Nil)
      val extraArgs = Try(config.getString("extraArgs")).toOption.getOrElse("")
      Host(hostname, keyFile, username, memory, cpu, scratch, extraArgs, gpu)
    }
  }

  implicit def fromConfig(implicit config: TasksConfig) =
    new SSHSettings

}

class SSHSettings(implicit config: TasksConfig) {
  import SSHSettings._

  val hosts: collection.mutable.Map[String, (Host, Boolean)] =
    collection.mutable.Map(config.sshHosts.asScala.map { case (_, value) =>
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
      new File(System.getProperty("user.home") + "/.ssh/known_hosts")
    )
    def verifyServerHostKey(
        hostname: String,
        port: Int,
        serverHostKeyAlgorithm: String,
        serverHostKey: Array[Byte]
    ) =
      kh.verifyHostkey(
        hostname,
        serverHostKeyAlgorithm,
        serverHostKey
      ) == KnownHosts.HOSTKEY_IS_OK
  }

  def terminateProcess(host: SSHSettings.Host, pid: String): Unit = {
    openSession(host) { session =>
      session.execCommand(s"kill $pid")
    }
  }

}

class SSHShutdown(implicit config: TasksConfig) extends ShutdownNode {

  val settings = SSHSettings.fromConfig

  def shutdownRunningNode(nodeName: RunningJobId): Unit = {
    val hostname = nodeName.value.split(":")(0)
    val pid = nodeName.value.split(":")(1)
    SSHOperations.terminateProcess(settings.hosts(hostname)._1, pid)
    settings.enableHost(hostname)
  }

  def shutdownPendingNode(nodeName: PendingJobId): Unit = ()

}

class SSHCreateNode(
    masterAddress: SimpleSocketAddress,
    codeAddress: CodeAddress
)(implicit
    config: TasksConfig,
    elasticSupport: ElasticSupportFqcn
) extends CreateNode {

  val settings = SSHSettings.fromConfig

  def requestOneNewJobFromJobScheduler(
      requestSize: ResourceRequest
  ): Try[(PendingJobId, ResourceAvailable)] =
    settings.hosts
      .filter(x => x._2._2 == true)
      .filter(x =>
        x._2._1.cpu >= requestSize.cpu._1 && x._2._1.memory >= requestSize.memory
      )
      .iterator
      .map { case (name, (host, _)) =>
        val script = Deployment.script(
          memory = host.memory,
          cpu = host.cpu,
          scratch = host.scratch,
          gpus = host.gpu,
          elasticSupport = elasticSupport,
          masterAddress = masterAddress,
          download = Uri(
            scheme = "http",
            hostname = codeAddress.address.getHostName,
            port = codeAddress.address.getPort,
            path = "/"
          ),
          slaveHostname = Some(host.hostname),
          background = true
        )
        SSHOperations.openSession(host) { session =>
          val command =
            "source .bash_profile; " + script

          session.execCommand(command)

          session.getStdin.close

          session.waitForCondition(
            ch.ethz.ssh2.ChannelCondition.STDOUT_DATA,
            10000
          )

          val stdout =
            scala.io.Source.fromInputStream(session.getStdout).mkString

          val pid = stdout.trim.toInt

          settings.disableHost(name)

          (
            PendingJobId(host.hostname + ":" + pid.toString),
            ResourceAvailable(
              cpu = host.cpu,
              memory = host.memory,
              scratch = host.scratch,
              gpu = host.gpu
            )
          )

        }
      }
      .find(_.isSuccess)
      .getOrElse(Failure(new RuntimeException("No enabled/working hosts")))

}

class SSHCreateNodeFactory(implicit
    config: TasksConfig,
    elasticSupport: ElasticSupportFqcn
) extends CreateNodeFactory {
  def apply(master: SimpleSocketAddress, codeAddress: CodeAddress) =
    new SSHCreateNode(master, codeAddress)
}

object SSHGetNodeName extends GetNodeName {
  def getNodeName = {
    val pid = java.lang.management.ManagementFactory
      .getRuntimeMXBean()
      .getName()
      .split("@")
      .head
    pid
  }
}

object SSHElasticSupport extends ElasticSupportFromConfig {
  implicit val fqcn = ElasticSupportFqcn("tasks.elastic.sh.SSHElasticSupport")
  def apply(implicit config: TasksConfig) = SimpleElasticSupport(
    fqcn = fqcn,
    hostConfig = None,
    reaperFactory = None,
    shutdown = new SSHShutdown,
    createNodeFactory = new SSHCreateNodeFactory,
    getNodeName = SSHGetNodeName
  )
}
