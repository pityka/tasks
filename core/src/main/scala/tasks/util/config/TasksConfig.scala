/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Modified work, Copyright (c) 2016 Istvan Bartha

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

package tasks.util.config

import com.typesafe.config.Config
import scala.collection.JavaConverters._
import java.io.File

class TasksConfig(val raw: Config) {

  val asString = raw.root.render

  val codeVersion = tasks.shared.CodeVersion(raw.getString("tasks.codeVersion"))

  val cacheEnabled = raw.getBoolean("tasks.cache.enabled")

  val askInterval: FD = raw.getDuration("tasks.askInterval")

  val launcherActorHeartBeatInterval: FD =
    raw.getDuration("tasks.failuredetector.heartbeat-interval")

  val fileSendChunkSize = raw.getBytes("tasks.fileSendChunkSize").toInt

  val resubmitFailedTask = raw.getBoolean("tasks.resubmitFailedTask")

  val verifySharedFileInCache = raw.getBoolean("tasks.verifySharedFileInCache")

  val disableRemoting = raw.getBoolean("tasks.disableRemoting")

  val nonLocalFileSystems = raw
    .getStringList("tasks.nonLocalFileSystems")
    .asScala
    .map(f => new java.io.File(f))

  val skipContentHashVerificationAfterCache =
    raw.getBoolean("tasks.skipContentHashVerificationAfterCache")

  val acceptableHeartbeatPause: FD =
    raw.getDuration("tasks.failuredetector.acceptable-heartbeat-pause")

  val hostNumCPU = raw.getInt("hosts.numCPU")

  val hostRAM = raw.getInt("hosts.RAM")

  val hostName = raw.getString("hosts.hostname")

  val hostPort = raw.getInt("hosts.port")

  val masterAddress =
    if (raw.hasPath("hosts.master")) {
      val h = raw.getString("hosts.master").split(":")(0)
      val p = raw.getString("hosts.master").split(":")(1).toInt
      Some(new java.net.InetSocketAddress(h, p))
    } else None

  val startApp = raw.getBoolean("hosts.app")

  val auxThreads = raw.getInt("tasks.auxThreads")

  val storageURI =
    new java.net.URI(raw.getString("tasks.fileservice.storageURI"))

  val fileServiceExtendedFolders = raw
    .getStringList("tasks.fileservice.extendedFolders")
    .asScala
    .map(x => new File(x))
    .toList
    .filter(_.isDirectory)

  val fileServiceThreadPoolSize =
    raw.getInt("tasks.fileservice.threadPoolSize")

  val sshHosts = raw.getObject("tasks.elastic.ssh.hosts")

  val elasticSupport = raw.getString("tasks.elastic.engine")

  val idleNodeTimeout: FD = raw.getDuration("tasks.elastic.idleNodeTimeout")

  val maxNodes = raw.getInt("tasks.elastic.maxNodes")

  val maxPendingNodes = raw.getInt("tasks.elastic.maxPending")

  val maxNodesCumulative = raw.getInt("tasks.elastic.maxNodesCumulative")

  val queueCheckInterval: FD =
    raw.getDuration("tasks.elastic.queueCheckInterval")

  val queueCheckInitialDelay: FD =
    raw.getDuration("tasks.elastic.queueCheckInitialDelay")

  val nodeKillerMonitorInterval: FD =
    raw.getDuration("tasks.elastic.nodeKillerMonitorInterval")

  val jvmMaxHeapFactor = raw.getDouble("tasks.elastic.jvmMaxHeapFactor")

  val logQueueStatus = raw.getBoolean("tasks.elastic.logQueueStatus")

  val endpoint: String = raw.getString("tasks.elastic.aws.endpoint")

  val spotPrice: Double = raw.getDouble("tasks.elastic.aws.spotPrice")

  val amiID: String = raw.getString("tasks.elastic.aws.ami")

  val slaveInstanceType = raw.getString("tasks.elastic.aws.instanceType")

  val securityGroup: String = raw.getString("tasks.elastic.aws.securityGroup")

  val securityGroups: List[String] =
    raw.getStringList("tasks.elastic.aws.securityGroups").asScala.toList

  val subnetId = raw.getString("tasks.elastic.aws.subnetId")

  val keyName = raw.getString("tasks.elastic.aws.keyName")

  val additionalJavaCommandline =
    raw.getString("tasks.elastic.javaCommandline")

  val iamRole = {
    val s = raw.getString("tasks.elastic.aws.iamRole")
    if (s == "" || s == "-") None
    else Some(s)
  }

  val placementGroup: Option[String] =
    raw.getString("tasks.elastic.aws.placementGroup") match {
      case x if x == "" => None
      case x            => Some(x)
    }

  val s3Region = raw.getString("tasks.s3.region")

  val s3ServerSideEncryption = raw.getBoolean("tasks.s3.serverSideEncryption")

  val s3CannedAcl = raw.getStringList("tasks.s3.cannedAcls").asScala.toList

  val s3GrantFullControl = raw
    .getStringList("tasks.s3.grantFullControl")
    .asScala
    .grouped(2)
    .map(x => x(0) -> x(1))
    .toList

  val instanceTags = raw
    .getStringList("tasks.elastic.aws.tags")
    .asScala
    .grouped(2)
    .map(x => x(0) -> x(1))
    .toList

  val terminateMaster = raw.getBoolean("tasks.elastic.aws.terminateMaster")

  val actorSystemName = raw.getString("tasks.akka.actorsystem.name")

  val addShutdownHook = raw.getBoolean("tasks.addShutdownHook")

}
