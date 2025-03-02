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
import com.typesafe.config.Config
import scala.jdk.CollectionConverters._
import tasks.shared.ResourceAvailable
import com.typesafe.config.ConfigRenderOptions

trait ConfigValuesForHostConfiguration {
  def raw: Config
  def hostImage =
    if (raw.hasPath("hosts.image")) Some(raw.getString("hosts.image")) else None

  def hostNumCPU = raw.getInt("hosts.numCPU")

  def hostGPU = raw.getIntList("hosts.gpus").asScala.toList.map(_.toInt) ++ raw
    .getString("hosts.gpusAsCommaString")
    .split(",")
    .toList
    .filter(_.nonEmpty)
    .map(_.toInt)

  def hostRAM = raw.getInt("hosts.RAM")

  def hostScratch = raw.getInt("hosts.scratch")

  def hostName = raw.getString("hosts.hostname")
  def hostNameExternal =
    if (raw.hasPath("hosts.hostnameExternal"))
      Some(raw.getString("hosts.hostnameExternal"))
    else None

  def hostPort = raw.getInt("hosts.port")
  def mayUseArbitraryPort = raw.getBoolean("hosts.mayUseArbitraryPort")

  def masterAddress =
    if (raw.hasPath("hosts.master")) {
      val h = raw.getString("hosts.master").split(":")(0)
      val p = raw.getString("hosts.master").split(":")(1).toInt
      Some(SimpleSocketAddress(h, p))
    } else None

  def startApp = raw.getBoolean("hosts.app")
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

  val asString = raw.root.render

  val codeVersion = tasks.shared.CodeVersion(raw.getString("tasks.codeVersion"))

  val cacheEnabled = raw.getBoolean("tasks.cache.enabled")

  val cachePath =
    raw.getString("tasks.cache.sharefilecache.path").toLowerCase match {
      case "prefix" => None
      case other =>
        Some(
          tasks.fileservice
            .FileServicePrefix(other.split("/").toVector.filter(_.nonEmpty))
        )
    }

  val askInterval: FD = raw.getDuration("tasks.askInterval")

  val launcherActorHeartBeatInterval: FD =
    raw.getDuration("tasks.failuredetector.heartbeat-interval")

  def fileSendChunkSize = raw.getBytes("tasks.fileSendChunkSize").toInt

  def resubmitFailedTask = raw.getBoolean("tasks.resubmitFailedTask")

  def verifySharedFileInCache = raw.getBoolean("tasks.verifySharedFileInCache")

  val disableRemoting = raw.getBoolean("tasks.disableRemoting")

  val workerWorkingDirectory =
    raw.getString("tasks.elastic.workerWorkingDirectory")

  val workerPackageName =
    raw.getString("tasks.elastic.workerPackageName")

  def skipContentHashVerificationAfterCache =
    raw.getBoolean("tasks.skipContentHashVerificationAfterCache")
  def skipContentHashCreationUponImport =
    raw.getBoolean("tasks.skipContentHashCreationUponImport")

  val acceptableHeartbeatPause: FD =
    raw.getDuration("tasks.failuredetector.acceptable-heartbeat-pause")

  

  val storageURI =
    new java.net.URI(raw.getString("tasks.fileservice.storageURI"))

  val proxyStorage = raw.getBoolean("tasks.fileservice.proxyStorage")

  val parallelismOfCacheAccessibilityCheck =
    raw.getInt("tasks.cache.accessibility-check-parallelism")

  def idleNodeTimeout: FD = raw.getDuration("tasks.elastic.idleNodeTimeout")

  def cacheTimeout: FD = raw.getDuration("tasks.cache.timeout")

  def maxNodes = raw.getInt("tasks.elastic.maxNodes")

  def maxPendingNodes = raw.getInt("tasks.elastic.maxPending")

  def maxNodesCumulative = raw.getInt("tasks.elastic.maxNodesCumulative")

  val queueCheckInterval: FD =
    raw.getDuration("tasks.elastic.queueCheckInterval")

  val queueCheckInitialDelay: FD =
    raw.getDuration("tasks.elastic.queueCheckInitialDelay")

  val nodeKillerMonitorInterval: FD =
    raw.getDuration("tasks.elastic.nodeKillerMonitorInterval")

  def jvmMaxHeapFactor = raw.getDouble("tasks.elastic.jvmMaxHeapFactor")

  def logQueueStatus = raw.getBoolean("tasks.elastic.logQueueStatus")

  

  def additionalJavaCommandline =
    raw.getString("tasks.elastic.javaCommandLine")

 

  val s3RegionProfileName =
    if (raw.hasPath("tasks.s3.regionProfileName"))
      Some(raw.getString("tasks.s3.regionProfileName"))
    else None

  val s3ServerSideEncryption = raw.getString("tasks.s3.serverSideEncryption")

  val s3CannedAcl = raw.getStringList("tasks.s3.cannedAcls").asScala.toList

  val s3GrantFullControl = raw
    .getStringList("tasks.s3.grantFullControl")
    .asScala
    .toList

  val s3UploadParallelism = raw.getInt("tasks.s3.uploadParallelism")

  val httpRemoteEnabled = raw.getBoolean("tasks.fileservice.remote.http")
  val s3RemoteEnabled = raw.getBoolean("tasks.fileservice.remote.s3")

  

  val addShutdownHook = raw.getBoolean("tasks.addShutdownHook")

  val uiFqcn = raw.getString("tasks.ui.fqcn")

  val uiServerHost = raw.getString("tasks.ui.queue.host")

  val uiServerPort = raw.getInt("tasks.ui.queue.port")

  val appUIServerHost = raw.getString("tasks.ui.app.host")

  val appUIServerPort = raw.getInt("tasks.ui.app.port") 

  

  val workerMainClass = raw.getString("tasks.worker-main-class")

  val createFilePrefixForTaskId =
    raw.getBoolean("tasks.createFilePrefixForTaskId")

  def allowDeletion = raw.getBoolean("tasks.fileservice.allowDeletion")

  def allowOverwrite = raw.getBoolean("tasks.fileservice.allowOverwrite")

  def folderFileStorageCompleteFileCheck =
    raw.getBoolean("tasks.fileservice.folderFileStorageCompleteFileCheck")

  def pendingNodeTimeout = raw.getDuration("tasks.elastic.pendingNodeTimeout")

  val checkTempFolderOnSlaveInitialization =
    raw.getBoolean("tasks.elastic.checktempfolder")

  val trackerFqcn = raw.getString("tasks.tracker.fqcn")

  val resourceUtilizationLogFile = raw.getString("tasks.tracker.logFile")

  def trackDataFlow = raw.getBoolean("tasks.queue.trackDataFlow")

  val parallelismOfReadingHistoryFiles =
    raw.getInt("tasks.queue.track-data-flow-history-file-read-parallelism")

  val saveTaskDescriptionInCache =
    raw.getBoolean("tasks.cache.saveTaskDescription")

  val writeFileHistories =
    raw.getBoolean("tasks.fileservice.writeFileHistories")

  val shWorkDir = raw.getString("tasks.elastic.sh.workdir")

  val connectToProxyFileServiceOnMain =
    raw.getBoolean("tasks.fileservice.connectToProxy")

  val storageEncryptionKey =
    if (raw.hasPath("tasks.fileservice.encryptionKey"))
      Some(raw.getString("tasks.fileservice.encryptionKey"))
    else None

}
