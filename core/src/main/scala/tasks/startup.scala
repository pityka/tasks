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

package tasks

import tasks.caching.kvstore._
import tasks.caching._
import tasks.queue._
import tasks.deploy._
import tasks.util._
import tasks.util.eq._
import tasks.fileservice._
import tasks.elastic._
import tasks.elastic.ec2._
import tasks.elastic.ssh._
import tasks.shared._
import tasks.shared.monitor._

import akka.actor._
import akka.util.Timeout
import akka.io.IO
import akka.pattern.ask
import akka.stream.ActorMaterializer

import spray.can.Http

import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.InetAddress
import java.io.File

import com.typesafe.config.{ConfigFactory, Config}
import scala.concurrent.Await
import collection.JavaConversions._

import scala.concurrent.duration._
import scala.concurrent._

import com.bluelabs.s3stream.S3StreamQueued

case class TaskSystemComponents(
    queue: QueueActor,
    fs: FileServiceActor,
    actorsystem: ActorSystem,
    cache: CacheActor,
    nodeLocalCache: NodeLocalCacheActor,
    filePrefix: FileServicePrefix,
    executionContext: ExecutionContext,
    actorMaterializer: ActorMaterializer
) {

  def childPrefix(name: String) =
    this.copy(filePrefix = this.filePrefix.append(name))
}

class TaskSystem private[tasks] (val hostConfig: MasterSlaveConfiguration,
                                 val system: ActorSystem) {

  implicit val as = system
  implicit val mat = ActorMaterializer()
  import as.dispatcher
  implicit val s3Stream = scala.util
    .Try(new S3StreamQueued(S3Helpers.credentials, config.global.s3Region))
    .toOption
  implicit val sh = new StreamHelper(s3Stream)

  private val tasksystemlog = akka.event.Logging(as, "TaskSystem")

  tasksystemlog.info("Listening on: " + hostConfig.myAddress.toString)
  tasksystemlog.info("CPU: " + hostConfig.myCardinality.toString)
  tasksystemlog.info("RAM: " + hostConfig.availableMemory.toString)
  tasksystemlog.info("Role: " + hostConfig.myRole.toString)
  tasksystemlog.info("Elastic: " + tasks.elastic.elasticSupport)

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
    (queueActor ? GetQueueInformation).onSuccess {
      case queue: QueueInfo => tasksystemlog.info("Queue content: " + queue)
    }

  }

  private val numberOfCores: Int = hostConfig.myCardinality

  private val availableMemory: Int = hostConfig.availableMemory

  private val host: InetSocketAddress = hostConfig.myAddress

  private lazy val masterAddress = hostConfig.master

  private val isLauncherOnly = hostConfig.myRole == SLAVE

  val reaperActor = elastic.elasticSupport match {
    case Some(EC2Grid) =>
      system.actorOf(Props(new EC2Reaper(config.global.terminateMaster)),
                     name = "reaper")
    case _ =>
      system.actorOf(Props[ProductionReaper], name = "reaper")
  }

  val remotenoderegistry =
    if (isLauncherOnly && elastic.elasticSupport.isDefined) {
      val remotepath =
        s"akka.tcp://tasks@${masterAddress.getHostName}:${masterAddress.getPort}/user/noderegistry"
      val noderegistry = Await.result(
          system.actorSelection(remotepath).resolveOne(600 seconds),
          atMost = 600 seconds)
      tasksystemlog.info("NodeRegistry: " + noderegistry)
      Some(noderegistry)
    } else None

  val remoteFileStorage = new RemoteFileStorage

  val managedFileStorage: Option[ManagedFileStorage] =
    if (config.global.storageURI.toString == "") None
    else {
      val s3bucket =
        if (config.global.storageURI.getScheme != null && config.global.storageURI.getScheme == "s3") {
          Some(
              (config.global.storageURI.getAuthority,
               config.global.storageURI.getPath.drop(1)))
        } else None

      if (s3bucket.isDefined) {
        val actorsystem = 1 // shade implicit conversion
        import as.dispatcher
        Some(new S3Storage(s3bucket.get._1, s3bucket.get._2, s3Stream.get))
      } else {
        val folder1Path =
          if (config.global.storageURI.getScheme == null)
            config.global.storageURI.getPath
          else if (config.global.storageURI.getScheme == "file")
            config.global.storageURI.getPath
          else {
            tasksystemlog.error(
                s"${config.global.storageURI} unknown protocol, use s3://bucket/key or file:/// (with absolute path), or just a plain path string (absolute or relative")
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
        val folders2 = config.global.fileServiceExtendedFolders
        val centralized = config.global.fileServiceBaseFolderIsShared
        if (folder1.list.size != 0) {
          tasksystemlog.warning(
              s"fileServiceBaseFolder (${folder1.getCanonicalPath}) is not empty. This is only safe if you restart a pipeline. ")
        }
        Some(new FolderFileStorage(folder1, centralized, folders2))
      }
    }

  tasksystemlog.info("FileStore: " + managedFileStorage)

  val fileActor = try {
    if (!isLauncherOnly) {

      val threadpoolsize = config.global.fileServiceThreadPoolSize

      val ac = system.actorOf(
          Props(new FileService(managedFileStorage.get, threadpoolsize))
            .withDispatcher("my-pinned-dispatcher"),
          "fileservice")
      reaperActor ! WatchMe(ac)
      ac
    } else {
      val actorpath =
        s"akka.tcp://tasks@${masterAddress.getHostName}:${masterAddress.getPort}/user/fileservice"
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

  val fileServiceActor =
    FileServiceActor(fileActor, managedFileStorage, remoteFileStorage)

  val nodeLocalCacheActor = system.actorOf(
      Props[NodeLocalCache].withDispatcher("my-pinned-dispatcher"),
      name = "nodeLocalCache")

  reaperActor ! WatchMe(nodeLocalCacheActor)

  val nodeLocalCache = NodeLocalCacheActor(nodeLocalCacheActor)

  val cacherActor = try {
    if (!isLauncherOnly && hostConfig.cacheAddress.isEmpty) {

      val cache: Cache = config.global.cacheEnabled match {
        case true => {
          config.global.cacheType match {
            case "sharedfile" =>
              new SharedFileCache()(fileServiceActor,
                                    nodeLocalCache,
                                    system,
                                    system.dispatcher)
            case other =>
              val store = new LevelDBWrapper(new File(config.global.cachePath))
              new KVCache(store,
                          akka.serialization.SerializationExtension(system))
          }

        }
        case false => new DisabledCache
      }
      val ac = system.actorOf(
          Props(new TaskResultCache(cache, fileServiceActor))
            .withDispatcher("my-pinned-dispatcher"),
          "cache")
      reaperActor ! WatchMe(ac)
      ac
    } else {
      if (hostConfig.cacheAddress.isEmpty) {
        val actorpath =
          s"akka.tcp://tasks@${masterAddress.getHostName}:${masterAddress.getPort}/user/cache"
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

  val queueActor = try {
    if (!isLauncherOnly) {
      val ac =
        system.actorOf(Props[TaskQueue].withDispatcher("taskqueue"), "queue")
      reaperActor ! WatchMe(ac)
      ac
    } else {
      val actorpath =
        s"akka.tcp://tasks@${masterAddress.getHostName}:${masterAddress.getPort}/user/queue"
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

  val elasticSupportFactory = elastic.elasticSupport.map(
      es =>
        es(master = hostConfig.master,
           queueActor = queueActor,
           resource = CPUMemoryAvailable(cpu = hostConfig.myCardinality,
                                         memory = hostConfig.availableMemory)))

  // start up noderegistry
  val noderegistry: Option[ActorRef] =
    if (!isLauncherOnly && elasticSupportFactory.isDefined) {

      val props = Props(elasticSupportFactory.get.createRegistry)

      val ac = system
        .actorOf(props.withDispatcher("my-pinned-dispatcher"), "noderegistry")

      reaperActor ! WatchMe(ac)

      Some(ac)
    } else None

  val packageServer: Option[ActorRef] =
    if (!isLauncherOnly && elasticSupportFactory.isDefined) {

      val pack = Deployment.pack
      tasksystemlog
        .info("Written executable package to: {}", pack.getAbsolutePath)

      val service =
        system.actorOf(Props(new PackageServerActor(pack)), "deliver-service")

      val actorsystem = 1 //shade implicit conversion

      IO(Http) ! Http.Bind(service, "0.0.0.0", port = host.getPort + 1)

      Some(service)

    } else None

  private val auxFjp = tasks.util.concurrent
    .newJavaForkJoinPoolWithNamePrefix("tasks-aux", config.global.auxThreads)
  private val auxExecutionContext =
    scala.concurrent.ExecutionContext.fromExecutorService(auxFjp)

  val components = TaskSystemComponents(queue = QueueActor(queueActor),
                                        fs = fileServiceActor,
                                        actorsystem = system,
                                        cache = CacheActor(cacherActor),
                                        nodeLocalCache = nodeLocalCache,
                                        filePrefix =
                                          FileServicePrefix(Vector()),
                                        executionContext = auxExecutionContext,
                                        actorMaterializer = mat)

  private val launcherActor = if (numberOfCores > 0) {
    val refresh = config.global.askInterval
    val ac = system.actorOf(
        Props(
            new TaskLauncher(queueActor,
                             nodeLocalCacheActor,
                             CPUMemoryAvailable(cpu = numberOfCores,
                                                memory = availableMemory),
                             refreshRate = refresh,
                             auxExecutionContext = auxExecutionContext,
                             actorMaterializer = mat,
                             remoteStorage = remoteFileStorage,
                             managedStorage = managedFileStorage))
          .withDispatcher("launcher"),
        "launcher")
    reaperActor ! WatchMe(ac)
    Some(ac)
  } else None

  if (isLauncherOnly && elasticSupportFactory.isDefined && launcherActor.isDefined) {
    val nodeName = getNodeName

    tasksystemlog.info(
        "This is a worker node. ElasticNodeAllocation is enabled. Node name: " + nodeName)

    remotenoderegistry.get ! NodeComingUp(
        Node(RunningJobId(nodeName),
             CPUMemoryAvailable(hostConfig.myCardinality,
                                hostConfig.availableMemory),
             launcherActor.get))

    val balancerHeartbeat = system.actorOf(
        Props(new HeartBeatActor(queueActor)).withDispatcher("heartbeat"),
        "heartbeatOf" + queueActor.path.address.toString
          .replace("://", "___") + queueActor.path.name)

    system.actorOf(
        Props(elasticSupportFactory.get.createSelfShutdown)
          .withDispatcher("my-pinned-dispatcher"))

  } else {
    tasksystemlog.warning("Nodename/jobname is not defined.")
  }

  private def initFailed {
    if (isLauncherOnly) {
      remotenoderegistry.foreach(_ ! InitFailed(PendingJobId(getNodeName)))
    }
  }

  var shuttingDown = false

  def shutdown = synchronized {
    if (hostConfig.myRole == MASTER) {
      if (!shuttingDown) {
        shuttingDown = true
        implicit val timeout = akka.util.Timeout(10 seconds)
        import system.dispatcher

        val latch = new java.util.concurrent.CountDownLatch(1)
        reaperActor ! Latch(latch)
        val cacheReaper = system.actorOf(Props(new CallbackReaper({
          fileActor ! PoisonPill
          system.actorSelection("/user/fileservice_*") ! PoisonPill
        })))
        (cacheReaper ? WatchMe(cacherActor)).foreach { _ =>
          cacherActor ! PoisonPillToCacheActor
          system.actorSelection("/user/cache_*") ! PoisonPillToCacheActor
        }
        queueActor ! PoisonPill

        launcherActor.foreach(_ ! PoisonPill)
        if (!isLauncherOnly) {
          noderegistry.foreach(_ ! PoisonPill)
        }
        nodeLocalCacheActor ! PoisonPill

        tasksystemlog.info("Shutting down tasksystem. Waiting for the latch.")
        latch.await
        auxFjp.shutdown
      }
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

  private def getNodeName: String = elasticSupportFactory.get.getNodeName

}
