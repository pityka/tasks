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

import tasks.util.SimpleSocketAddress
import org.ekrich.config.Config
import scala.jdk.CollectionConverters._
import tasks.shared.ResourceAvailable
import org.ekrich.config.ConfigRenderOptions

trait ConfigValuesForHostConfiguration {
  def raw: Config
  private[tasks] def hostImage =
    if (raw.hasPath("hosts.image")) Some(raw.getString("hosts.image")) else None

  def hostLabels: Set[String] = {
    val list =
      if (raw.hasPath("hosts.labels"))
        raw.getStringList("hosts.labels").asScala.toSet
      else Set.empty[String]
    val commaSeparated =
      if (raw.hasPath("hosts.labelsAsCommaString"))
        raw
          .getString("hosts.labelsAsCommaString")
          .split(",")
          .iterator
          .map(_.trim)
          .filter(_.nonEmpty)
          .toSet
      else Set.empty[String]
    list ++ commaSeparated
  }

  def hostNumCPU = raw.getInt("hosts.numCPU")

  private[tasks] def hostGPU = {
    val list = raw.getIntList("hosts.gpus").asScala.toList.map(_.toInt)
    val commaSeparated = raw
      .getString("hosts.gpusAsCommaString")
      .split(",")
      .toList
      .filter(_.nonEmpty)
      .map(_.toInt)
    (list ++ commaSeparated).distinct.sorted
  }

  def hostRAM = raw.getInt("hosts.RAM")

  def hostScratch = raw.getInt("hosts.scratch")

  def hostName = raw.getString("hosts.hostname")
  private[tasks] def hostNameExternal =
    if (raw.hasPath("hosts.hostnameExternal"))
      Some(raw.getString("hosts.hostnameExternal"))
    else None

  private[tasks] def hostPort = raw.getInt("hosts.port")
  private[tasks] def mayUseArbitraryPort = raw.getBoolean("hosts.mayUseArbitraryPort")

  private[tasks] def masterAddress =
    if (raw.hasPath("hosts.master")) {
      val h = raw.getString("hosts.master").split(":")(0)
      val p = raw.getString("hosts.master").split(":")(1).toInt
      Some(SimpleSocketAddress(h, p))
    } else None

  private[tasks] def masterPrefix = if (raw.hasPath("hosts.masterprefix")) {
    Some(raw.getString("hosts.masterprefix"))
  } else None

  private[tasks] def startApp = raw.getBoolean("hosts.app")
}

class TasksConfig(load: () => Config) extends ConfigValuesForHostConfiguration {

  private val lastLoadedAt =
    new java.util.concurrent.atomic.AtomicLong(System.nanoTime)
  private var lastLoaded = load()

  private def maxConfigLoadInterval =
    lastLoaded.getDuration("tasks.maxConfigLoadInterval").toNanos

  def raw: Config = {
    val currentTime = System.nanoTime
    if (false && currentTime - lastLoadedAt.get > maxConfigLoadInterval) {
      scribe.debug("Reload config.")
      val current = load()
      lastLoaded = current
      lastLoadedAt.set(currentTime)
      current
    } else lastLoaded
  }

  private[tasks] val asString = raw.root.render

  private[tasks] val codeVersion = tasks.shared.CodeVersion(raw.getString("tasks.codeVersion"))

  private[tasks] val cacheEnabled = raw.getBoolean("tasks.cache.enabled")

  private[tasks] val cachePath =
    raw.getString("tasks.cache.sharefilecache.path").toLowerCase match {
      case "prefix" => None
      case other =>
        Some(
          tasks.fileservice
            .FileServicePrefix(other.split("/").toVector.filter(_.nonEmpty))
        )
    }

  private[tasks] val askInterval: FD = raw.getDuration("tasks.askInterval")

  private[tasks] val resultPollInterval: FD = raw.getDuration("tasks.resultPollInterval")

  private[tasks] val askForWorkTimeout: FD =
    raw.getDuration("tasks.queue.askForWorkTimeout")

  private[tasks] val remoteMessengerServerIdleTimeout: FD =
    raw.getDuration("tasks.remoteMessenger.serverIdleTimeout")

  private[tasks] val remoteMessengerServerRequestHeaderReceiveTimeout: FD =
    raw.getDuration("tasks.remoteMessenger.serverRequestHeaderReceiveTimeout")

  private[tasks] val remoteMessengerClientTimeout: FD =
    raw.getDuration("tasks.remoteMessenger.clientTimeout")

  private[tasks] val remoteMessengerClientIdleConnectionTime: FD =
    raw.getDuration("tasks.remoteMessenger.clientIdleConnectionTime")

  private[tasks] val launcherActorHeartBeatInterval: FD =
    raw.getDuration("tasks.failuredetector.heartbeat-interval")

  private[tasks] def fileSendChunkSize = raw.getBytes("tasks.fileSendChunkSize").toInt

  private[tasks] def resubmitFailedTask = raw.getBoolean("tasks.resubmitFailedTask")

  private[tasks] def clearQueueStateWhenLastMainProcessExits =
    raw.getBoolean("tasks.clearQueueStateWhenLastMainProcessExits")

  private[tasks] def clearQueueStateWhenMainProcessJoins =
    raw.getBoolean("tasks.clearQueueStateWhenMainProcessJoins")

  private[tasks] def verifySharedFileInCache = raw.getBoolean("tasks.verifySharedFileInCache")

  private[tasks] val disableRemoting = raw.getBoolean("tasks.disableRemoting")

  private[tasks] val workerWorkingDirectory =
    raw.getString("tasks.elastic.workerWorkingDirectory")

  private[tasks] val workerPackageName =
    raw.getString("tasks.elastic.workerPackageName")

  private[tasks] def skipContentHashVerificationAfterCache =
    raw.getBoolean("tasks.skipContentHashVerificationAfterCache")
  private[tasks] def skipContentHashCreationUponImport =
    raw.getBoolean("tasks.skipContentHashCreationUponImport")

  private[tasks] val storageURI =
    new java.net.URI(raw.getString("tasks.fileservice.storageURI"))

  private[tasks] val proxyStorage = raw.getBoolean("tasks.fileservice.proxyStorage")

  private[tasks] val parallelismOfCacheAccessibilityCheck =
    raw.getInt("tasks.cache.accessibility-check-parallelism")

  val nodeName = raw.getString("tasks.elastic.nodename")

  private[tasks] def zeroCPULauncherOnApp =
    raw.getBoolean("tasks.elastic.zeroCPULauncherOnApp")

  private[tasks] def idleNodeTimeout: FD = raw.getDuration("tasks.elastic.idleNodeTimeout")

  private[tasks] def cacheTimeout: FD = raw.getDuration("tasks.cache.timeout")

  private[tasks] def maxNodes = raw.getInt("tasks.elastic.maxNodes")

  private[tasks] def maxPendingNodes = raw.getInt("tasks.elastic.maxPending")

  private[tasks] def maxNodesCumulative = raw.getInt("tasks.elastic.maxNodesCumulative")

  private[tasks] def jvmMaxHeapFactor = raw.getDouble("tasks.elastic.jvmMaxHeapFactor")

  private[tasks] def logQueueStatus = raw.getBoolean("tasks.elastic.logQueueStatus")

  private[tasks] def nodeRequestFailedLogInterval: FD =
    raw.getDuration("tasks.elastic.nodeRequestFailedLogInterval")

  private[tasks] def additionalJavaCommandline =
    raw.getString("tasks.elastic.javaCommandLine")

  private[tasks] val s3RegionProfileName =
    if (raw.hasPath("tasks.s3.regionProfileName"))
      Some(raw.getString("tasks.s3.regionProfileName"))
    else None

  private[tasks] val s3ServerSideEncryption = raw.getString("tasks.s3.serverSideEncryption")

  private[tasks] val s3CannedAcl = raw.getStringList("tasks.s3.cannedAcls").asScala.toList

  private[tasks] val s3GrantFullControl = raw
    .getStringList("tasks.s3.grantFullControl")
    .asScala
    .toList

  private[tasks] val s3UploadParallelism = raw.getInt("tasks.s3.uploadParallelism")

  private[tasks] val s3UploadPartSizeMB = raw.getInt("tasks.s3.uploadPartSizeMB")

  private[tasks] val s3DownloadParallelism = raw.getInt("tasks.s3.downloadParallelism")

  private[tasks] val s3DownloadPartSizeMB = raw.getInt("tasks.s3.downloadPartSizeMB")

  private[tasks] val s3MultipartThreshold = raw.getLong("tasks.s3.multipartThreshold")

  private[tasks] val s3RetryMaxAttempts = raw.getInt("tasks.s3.retryMaxAttempts")

  private[tasks] val httpRemoteEnabled = raw.getBoolean("tasks.fileservice.remote.http")
  private[tasks] val s3RemoteEnabled = raw.getBoolean("tasks.fileservice.remote.s3")

  private[tasks] val addShutdownHook = raw.getBoolean("tasks.addShutdownHook")

  private[tasks] val otelMaxSeries = raw.getInt("tasks.otel.maxSeries")

  private[tasks] val workerMainClass = raw.getString("tasks.worker-main-class")

  private[tasks] val createFilePrefixForTaskId =
    raw.getBoolean("tasks.createFilePrefixForTaskId")

  private[tasks] def allowDeletion = raw.getBoolean("tasks.fileservice.allowDeletion")

  private[tasks] def allowOverwrite = raw.getBoolean("tasks.fileservice.allowOverwrite")

  private[tasks] def allowOverwriteFor(path: String): Boolean =
    allowOverwrite || {
      val patterns = raw
        .getStringList("tasks.fileservice.allowOverwriteRegexes")
        .asScala
        .toList
      patterns.exists(regex => path.matches(regex))
    }

  private[tasks] def folderFileStorageCompleteFileCheck =
    raw.getBoolean("tasks.fileservice.folderFileStorageCompleteFileCheck")

  private[tasks] def pendingNodeTimeout = scala.jdk.DurationConverters
    .JavaDurationOps(raw.getDuration("tasks.elastic.pendingNodeTimeout"))
    .toScala

  private[tasks] val checkTempFolderOnWorkerInitialization =
    raw.getBoolean("tasks.elastic.checktempfolder")

  def workerHealthUrlFile: Option[java.io.File] = {
    val v = raw.getString("tasks.worker.healthUrlFile")
    if (v.isEmpty || v == "-") None else Some(new java.io.File(v))
  }

  private[tasks] def trackDataFlow = raw.getBoolean("tasks.queue.trackDataFlow")

  private[tasks] val parallelismOfReadingHistoryFiles =
    raw.getInt("tasks.queue.track-data-flow-history-file-read-parallelism")

  private[tasks] val writeFileHistories =
    raw.getBoolean("tasks.fileservice.writeFileHistories")

  private[tasks] val connectToProxyFileServiceOnMain =
    raw.getBoolean("tasks.fileservice.connectToProxy")

  private[tasks] val storageEncryptionKey =
    if (raw.hasPath("tasks.fileservice.encryptionKey"))
      Some(raw.getString("tasks.fileservice.encryptionKey"))
    else None

}
