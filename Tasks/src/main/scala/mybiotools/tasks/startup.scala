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

package tasks

import tasks.caching.kvstore._
import tasks.caching._
import tasks.queue._
import tasks.deploy._
import tasks.util._
import tasks.fileservice._
import tasks.elastic._
import tasks.elastic.ec2._
import tasks.elastic.drmaa._
import tasks.elastic.lsf._
import tasks.elastic.ssh._
import tasks.shared._
import tasks.shared.monitor._

import akka.actor.{Actor, ActorRef, PoisonPill, ActorSystem, Props, ActorRefFactory}
import tasks._
import java.io.File
import akka.actor.Actor._

import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.InetAddress
import scala.concurrent.duration._
import scala.concurrent._
import akka.pattern.ask
import com.typesafe.config.{ConfigFactory, Config}
import scala.concurrent.Await
import collection.JavaConversions._
import akka.util.Timeout

case class TaskSystemComponents(
    val queue: QueueActor,
    val fs: FileServiceActor,
    val actorsystem: ActorSystem,
    val cache: CacheActor,
    val nodeLocalCache: NodeLocalCacheActor,
    val filePrefix: FileServicePrefix
) {

  def getLogger(caller: AnyRef) = getApplicationLogger(caller)(actorsystem)

  def registerApplicationFileLogger(f: File) = {
    val fileLogger =
      actorsystem.actorOf(Props(new FileLogger(f, Some("APPLICATION"))))
    val fileLoggerAll = actorsystem.actorOf(
        Props(new FileLogger(new File(f.getAbsolutePath + ".log.all"))))
    actorsystem.eventStream
      .subscribe(fileLogger, classOf[akka.event.Logging.LogEvent])
    actorsystem.eventStream
      .subscribe(fileLoggerAll, classOf[akka.event.Logging.LogEvent])
    fileLogger
  }

  def childPrefix(name: String) =
    this.copy(filePrefix = this.filePrefix.append(name))
}

class TaskSystem private[tasks] (val hostConfig: MasterSlaveConfiguration,
                                 config: Config) {

  val configuration = {
    val actorProvider = hostConfig match {
      case x: LocalConfiguration => "akka.actor.LocalActorRefProvider"
      case _ => "akka.remote.RemoteActorRefProvider"
    }

    val numberOfAkkaRemotingThreads = if (hostConfig.myRole == MASTER) 6 else 2

    val configSource = s"""
task-worker-blocker-dispatcher.fork-join-executor.parallelism-max = ${hostConfig.myCardinality}
task-worker-blocker-dispatcher.fork-join-executor.parallelism-min = ${hostConfig.myCardinality}
akka {
  actor {
    provider = "${actorProvider}"
  }
  remote {
    enabled-transports = ["akka.remote.netty.tcp"]
    netty.tcp {
      hostname = "${hostConfig.myAddress.getHostName}"
      port = ${hostConfig.myAddress.getPort.toString}
      server-socket-worker-pool.pool-size-max = ${numberOfAkkaRemotingThreads}
      client-socket-worker-pool.pool-size-max = ${numberOfAkkaRemotingThreads}
    }
 }
}
  """ + (if (tasks.util.config.logToStandardOutput)
           """
    akka.loggers = ["akka.event.Logging$DefaultLogger"]
    """
         else "")

    val customConf = ConfigFactory.parseString(configSource)

    val classpathConf = ConfigFactory.load("akkaoverrides.conf")

    customConf.withFallback(config).withFallback(classpathConf)

  }

  val system = ActorSystem("tasks", configuration)

  val logger = system.actorOf(Props(new LogPublishActor(None)))
  system.eventStream.subscribe(logger, classOf[akka.event.Logging.LogEvent])

  private val tasksystemlog = tasks.getLogger(this)(system)

  def registerApplicationFileLogger(f: File) =
    components.registerApplicationFileLogger(f)

  def registerFileLogger(f: File) = {
    val fileLogger = system.actorOf(Props(new FileLogger(f)))
    system.eventStream
      .subscribe(fileLogger, classOf[akka.event.Logging.LogEvent])
    fileLogger
  }

  def registerFileLoggerToErrorStream(f: File) = {
    val fileLogger = system.actorOf(Props(new FileLogger(f)))
    system.eventStream.subscribe(fileLogger, classOf[akka.event.Logging.Error])
    fileLogger
  }

  def getLogger(caller: AnyRef) = getApplicationLogger(caller)(system)

  val ElasticNodeAllocationEnabled = config.getBoolean("tasks.elastic.enabled")

  val logFile: Option[File] = config.getString("tasks.logFile") match {
    case x if x == "" => None
    case x => Some(new File(x + System.currentTimeMillis.toString + ".log"))
  }

  logFile.foreach { f =>
    registerFileLogger(f)
    registerFileLoggerToErrorStream(new File(f.getCanonicalPath + ".errors"))
  }

  tasksystemlog.info("Listening on: " + hostConfig.myAddress.toString)
  tasksystemlog.info("CPU: " + hostConfig.myCardinality.toString)
  tasksystemlog.info("RAM: " + hostConfig.availableMemory.toString)
  tasksystemlog.info("Role: " + hostConfig.myRole.toString)

  if (hostConfig.myCardinality > Runtime.getRuntime().availableProcessors()) {
    tasksystemlog.warning(
        "Number of CPUs in the machine is " + Runtime
          .getRuntime()
          .availableProcessors + ". numCPU should not be greater than this.")
  }

  tasksystemlog.info("Master node address is: " + hostConfig.master.toString)

  def printQueueToLog {
    implicit val timeout = akka.util.Timeout(10 seconds)
    import system.dispatcher
    (balancerActor ? GetQueueInformation).onSuccess {
      case queue: QueueInfo => tasksystemlog.info("Queue content: " + queue)
    }

  }

  private val numberOfCores: Int = hostConfig.myCardinality

  private val availableMemory: Int = hostConfig.availableMemory

  private val host: InetSocketAddress = hostConfig.myAddress

  private lazy val balancerAndCacherAddress = hostConfig.master

  private val isLauncherOnly = hostConfig.myRole == SLAVE

  val gridengine = config.getString("hosts.gridengine") match {
    case x if x == "EC2" => EC2Grid
    case x if x == "LSF" => LSFGrid
    case x if x == "SGE" => SGEGrid
    case x if x == "SSH" => SSHGrid
    case _ => NoGrid
  }

  if (!isLauncherOnly && ElasticNodeAllocationEnabled && gridengine == SGEGrid) {
    DRMAA.testDRMAAConnection
  }

  val reaperActor = gridengine match {
    case EC2Grid =>
      system.actorOf(
          Props(
              new EC2Reaper(
                  logFile.toList,
                  config.getString("tasks.elastic.aws.fileStoreBucket"),
                  config.getString(
                      "tasks.elastic.aws.fileStoreBucketFolderPrefix"))),
          name = "reaper")
    case LSFGrid =>
      system.actorOf(Props(new LSFReaper(RunningJobId(getNodeName))),
                     name = "reaper")
    case SGEGrid =>
      system.actorOf(Props(new SGEReaper(RunningJobId(getNodeName))),
                     name = "reaper")
    case _ =>
      system
        .actorOf(Props[ProductionReaper], name = "reaper") // this also works for SSHgrid
  }

  val remotenoderegistry =
    if (isLauncherOnly && ElasticNodeAllocationEnabled) {
      val remotepath =
        s"akka.tcp://tasks@${balancerAndCacherAddress.getHostName}:${balancerAndCacherAddress.getPort}/user/noderegistry"
      val noderegistry = Await.result(
          system.actorSelection(remotepath).resolveOne(600 seconds),
          atMost = 600 seconds)
      tasksystemlog.info("NodeRegistry: " + noderegistry)
      Some(noderegistry)
    } else None

  val cacheFile: Option[File] = try {
    if (config.getBoolean("tasks.cacheEnabled") && !isLauncherOnly) {
      gridengine match {
        // case EC2Grid => throw new RuntimeException("LevelDB cache not implemented in EC2")
        case _ => Some(new File(config.getString("tasks.cacheFile")))
      }
    } else None
  } catch {
    case e: Throwable => {
      initFailed
      throw e
    }
  }

  if (gridengine == EC2Grid && !isLauncherOnly) {
    // system.actorOf(Props(new S3Updater(cacheFile.toList ++ logFile.toList, config.getString("tasks.fileServiceBaseFolder"))), name = "s3updater")
  }

  val storageURI =
    new java.net.URI(config.getString("tasks.fileService.storageURI"))
  val s3bucket =
    if (storageURI.getScheme != null && storageURI.getScheme == "s3") {
      Some((storageURI.getAuthority, storageURI.getPath.drop(1)))
    } else None

  val fileActor = try {
    if (!isLauncherOnly) {

      val filestore = gridengine match {
        case EC2Grid if s3bucket.isDefined => {
          new S3Storage(s3bucket.get._1, s3bucket.get._2)
        }
        case _ => {
          val folder1Path =
            if (storageURI.getScheme == null) storageURI.getPath
            else if (storageURI.getScheme == "file") storageURI.getPath
            else {
              tasksystemlog.error(
                  s"$storageURI unknown protocol, use s3://bucket/key or file:/// (with absolute path), or just a plain path string (absolute or relative")
              System.exit(1)
              throw new RuntimeException("dsf")
            }
          val folder1 = new File(folder1Path).getCanonicalFile
          if (folder1.isFile) {
            tasksystemlog.error(s"$folder1 is a file. Calling System.exit(1)")
            System.exit(1)
          }
          if (!folder1.isDirectory) {
            tasksystemlog.warning(s"Folder $folder1 does not exists. mkdir ")
            folder1.mkdirs
          }
          val folders2 = config
            .getStringList("tasks.fileServiceExtendedFolders")
            .map(x => new File(x).getCanonicalFile)
            .toList
            .filter(_.isDirectory)
          val centralized =
            config.getBoolean("tasks.fileServiceBaseFolderIsShared")
          if (folder1.list.size != 0) {
            tasksystemlog.warning(
                s"fileServiceBaseFolder (${folder1.getCanonicalPath}) is not empty. This is only safe if you restart a pipeline. ")
          }
          new FolderFileStorage(folder1, centralized, folders2)
        }
      }

      tasksystemlog.info("FileStore: " + filestore)

      val threadpoolsize = config.getInt("tasks.fileServiceThreadPoolSize")

      val fileListPath = new File(
          config.getString("tasks.fileServiceFileList"))

      val fileList = new FileList(new DirectLevelDBWrapper(fileListPath))

      val ac = system.actorOf(
          Props(new FileService(filestore, fileList, threadpoolsize))
            .withDispatcher("my-pinned-dispatcher"),
          "fileservice")
      reaperActor ! WatchMe(ac)
      ac
    } else {
      val actorpath =
        s"akka.tcp://tasks@${balancerAndCacherAddress.getHostName}:${balancerAndCacherAddress.getPort}/user/fileservice"
      val globalFileService = Await.result(
          system.actorSelection(actorpath).resolveOne(600 seconds),
          atMost = 600 seconds)

      tasksystemlog.info("FileService: " + globalFileService)
      globalFileService
    }
  } catch {
    case e: Throwable => {
      initFailed
      throw e
    }
  }

  val cacherActor = try {
    if (!isLauncherOnly && hostConfig.cacheAddress.isEmpty) {

      val cache: Cache = config.getBoolean("tasks.cacheEnabled") match {
        case true => {
          val store = config.getString("tasks.cache.store") match {
            case "leveldb" => new LevelDBWrapper(cacheFile.get)
            case "filesystem" => new FileSystemLargeKVStore(cacheFile.get)
          }
          new KVCache(store, akka.serialization.SerializationExtension(system))
        }
        case false => new DisabledCache
      }
      val ac = system.actorOf(
          Props(new TaskResultCache(cache, FileServiceActor(fileActor)))
            .withDispatcher("my-pinned-dispatcher"),
          "cache")
      reaperActor ! WatchMe(ac)
      ac
    } else {
      if (hostConfig.cacheAddress.isEmpty) {
        val actorpath =
          s"akka.tcp://tasks@${balancerAndCacherAddress.getHostName}:${balancerAndCacherAddress.getPort}/user/cache"
        Await.result(system.actorSelection(actorpath).resolveOne(600 seconds),
                     atMost = 600 seconds)
      } else {
        val actorpath =
          s"akka.tcp://tasks@${hostConfig.cacheAddress.get.getHostName}:${hostConfig.cacheAddress.get.getPort}/user/cache"
        Await.result(system.actorSelection(actorpath).resolveOne(600 seconds),
                     atMost = 600 seconds)
      }
    }
  } catch {
    case e: Throwable => {
      initFailed
      throw e
    }
  }

  val balancerActor = try {
    if (!isLauncherOnly) {
      val ac =
        system.actorOf(Props[TaskQueue].withDispatcher("taskqueue"), "queue")
      reaperActor ! WatchMe(ac)
      ac
    } else {
      val actorpath =
        s"akka.tcp://tasks@${balancerAndCacherAddress.getHostName}:${balancerAndCacherAddress.getPort}/user/queue"
      val ac = Await.result(
          system.actorSelection(actorpath).resolveOne(600 seconds),
          atMost = 600 seconds)
      tasksystemlog.info("Queue: " + ac)
      ac
    }
  } catch {
    case e: Throwable => {
      initFailed
      throw e
    }
  }

  // start up noderegistry
  val noderegistry: Option[ActorRef] =
    if (!isLauncherOnly && ElasticNodeAllocationEnabled) {
      val resource = CPUMemoryAvailable(cpu = hostConfig.myCardinality,
                                        memory = hostConfig.availableMemory)
      val ac = gridengine match {
        case LSFGrid =>
          system.actorOf(Props(
                             new LSFNodeRegistry(hostConfig.myAddress,
                                                 balancerActor,
                                                 resource))
                           .withDispatcher("my-pinned-dispatcher"),
                         "noderegistry")
        case SGEGrid =>
          system.actorOf(Props(
                             new SGENodeRegistry(hostConfig.myAddress,
                                                 balancerActor,
                                                 resource))
                           .withDispatcher("my-pinned-dispatcher"),
                         "noderegistry")
        case EC2Grid =>
          system.actorOf(Props(
                             new EC2NodeRegistry(hostConfig.myAddress,
                                                 balancerActor,
                                                 resource))
                           .withDispatcher("my-pinned-dispatcher"),
                         "noderegistry")
        case SSHGrid =>
          system.actorOf(Props(
                             new SSHNodeRegistry(hostConfig.myAddress,
                                                 balancerActor,
                                                 resource))
                           .withDispatcher("my-pinned-dispatcher"),
                         "noderegistry")
        case NoGrid =>
          throw new RuntimeException(
              "ElasticNodeAllocation and NoGrid are incompatible")
      }

      reaperActor ! WatchMe(ac)

      Some(ac)
    } else None

  val queueActor = QueueActor(balancerActor)

  val fileServiceActor = FileServiceActor(fileActor)

  val nodeLocalCacheActor = system.actorOf(
      Props[NodeLocalCache].withDispatcher("my-pinned-dispatcher"),
      name = "nodeLocalCache")

  reaperActor ! WatchMe(nodeLocalCacheActor)

  val nodeLocalCache = NodeLocalCacheActor(nodeLocalCacheActor)

  implicit val components = TaskSystemComponents(
      queue = queueActor,
      fs = fileServiceActor,
      actorsystem = system,
      cache = CacheActor(cacherActor),
      nodeLocalCache = nodeLocalCache,
      filePrefix = FileServicePrefix(Vector())
  )

  private val launcherActor = if (numberOfCores > 0) {
    val refresh: FiniteDuration = (new DurationLong(
        config.getMilliseconds("tasks.askInterval")).millisecond)
    val ac = system.actorOf(
        Props(
            new TaskLauncher(balancerActor,
                             nodeLocalCacheActor,
                             CPUMemoryAvailable(cpu = numberOfCores,
                                                memory = availableMemory),
                             refreshRate = refresh))
          .withDispatcher("launcher"),
        "launcher")
    reaperActor ! WatchMe(ac)
    Some(ac)
  } else None

  if (isLauncherOnly && ElasticNodeAllocationEnabled && launcherActor.isDefined) {
    val nodeName = getNodeName

    tasksystemlog.info(
        "This is worker node. ElasticNodeAllocation is enabled. Node name: " + nodeName)
    if (nodeName != null) {
      // ActorInEnvelope
      val acenv = launcherActor.get

      val resource = CPUMemoryAvailable(hostConfig.myCardinality,
                                        hostConfig.availableMemory)
      remotenoderegistry.get ! NodeComingUp(
          Node(RunningJobId(nodeName), resource, acenv))

      val balancerHeartbeat = system.actorOf(
          Props(new HeartBeatActor(balancerActor)).withDispatcher("heartbeat"),
          "heartbeatOf" + balancerActor.path.address.toString
            .replace("://", "___") + balancerActor.path.name)

      gridengine match {
        case LSFGrid =>
          system.actorOf(
              Props(new LSFSelfShutdown(RunningJobId(nodeName), balancerActor))
                .withDispatcher("my-pinned-dispatcher"))
        case SGEGrid =>
          system.actorOf(
              Props(new SGESelfShutdown(RunningJobId(nodeName), balancerActor))
                .withDispatcher("my-pinned-dispatcher"))
        case EC2Grid =>
          system.actorOf(
              Props(new EC2SelfShutdown(RunningJobId(nodeName), balancerActor))
                .withDispatcher("my-pinned-dispatcher"))
        case SSHGrid =>
          system.actorOf(
              Props(new SSHSelfShutdown(RunningJobId(nodeName), balancerActor))
                .withDispatcher("my-pinned-dispatcher"))
        case NoGrid =>
          throw new RuntimeException(
              "ElasticNodeAllocation and NoGrid are incompatible")

      }

    } else {
      tasksystemlog.warning("Nodename/jobname is not defined.")
    }
  }

  private def initFailed {
    if (isLauncherOnly) {
      remotenoderegistry.foreach(_ ! InitFailed(PendingJobId(getNodeName)))
    }
  }

  def shutdown {
    if (hostConfig.myRole == MASTER) {
      implicit val timeout = akka.util.Timeout(10 seconds)
      import system.dispatcher

      val latch = new java.util.concurrent.CountDownLatch(1)
      reaperActor ! Latch(latch)

      balancerActor ! PoisonPill
      cacherActor ! PoisonPill
      fileActor ! PoisonPill
      system.actorSelection("/user/fileservice_*") ! PoisonPill
      system.actorSelection("/user/cache_*") ! PoisonPill

      launcherActor.foreach(_ ! PoisonPill)
      if (!isLauncherOnly) {
        noderegistry.foreach(_ ! PoisonPill)
      }
      nodeLocalCacheActor ! PoisonPill
      latch.await
    } else {
      system.shutdown
    }

  }

  scala.sys.addShutdownHook {
    tasksystemlog.warning(
        "JVM is shutting down - will call tasksystem shutdown.")
    shutdown
    tasksystemlog.warning("JVM is shutting down - called tasksystem shutdown.")
  }

  private def getNodeName: String = gridengine match {
    case LSFGrid => System.getenv("LSB_JOBID")
    case SSHGrid => {
      val pid = java.lang.management.ManagementFactory
        .getRuntimeMXBean()
        .getName()
        .split("@")
        .head
      val hostname = tasks.util.config.hostName
      hostname + ":" + pid
    }
    case SGEGrid => System.getenv("JOB_ID")
    case EC2Grid => EC2Operations.readMetadata("instance-id").head
    case NoGrid =>
      throw new RuntimeException(
          "ElasticNodeAllocation and NoGrid are incompatible")
  }

}
