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

import tasks.caching._
import tasks.queue._
import tasks.deploy._
import tasks.util._
import tasks.util.config.TasksConfig
import tasks.fileservice._
import tasks.wire._
import tasks.elastic._
import tasks.shared._

import akka.actor._
import akka.pattern.ask
import akka.stream._

import java.io.File

import scala.concurrent.Await

import scala.concurrent.duration._
import scala.concurrent._
import scala.util._

import com.bluelabs.s3stream.S3ClientQueued

case class TaskSystemComponents(
    queue: QueueActor,
    fs: FileServiceComponent,
    actorsystem: ActorSystem,
    cache: CacheActor,
    nodeLocalCache: NodeLocalCacheActor,
    filePrefix: FileServicePrefix,
    executionContext: ExecutionContext,
    actorMaterializer: Materializer,
    tasksConfig: TasksConfig
) {

  def withChildPrefix(name: String) =
    this.copy(filePrefix = this.filePrefix.append(name))
}

class TaskSystem private[tasks] (
    val hostConfig: MasterSlaveConfiguration with HostConfiguration,
    val system: ActorSystem,
    val elasticSupport: Option[
      ElasticSupport[_ <: NodeCreatorImpl, _ <: SelfShutdown]])(
    implicit val config: TasksConfig) {

  implicit val AS = system
  implicit val AM = ActorMaterializer()
  import AS.dispatcher
  implicit val s3Stream = scala.util
    .Try(new S3ClientQueued(config.s3Region))
    .toOption
  implicit val streamHelper = new StreamHelper(s3Stream)

  private val tasksystemlog = akka.event.Logging(AS, "TaskSystem")

  tasksystemlog.info("Listening on: " + hostConfig.myAddress.toString)
  tasksystemlog.info("CPU: " + hostConfig.myCardinality.toString)
  tasksystemlog.info("RAM: " + hostConfig.availableMemory.toString)
  tasksystemlog.info("Roles: " + hostConfig.myRoles.mkString(", "))
  tasksystemlog.info("Elastic: " + elasticSupport)

  if (hostConfig.myCardinality > Runtime.getRuntime().availableProcessors()) {
    tasksystemlog.warning(
      "Number of CPUs in the machine is " + Runtime
        .getRuntime()
        .availableProcessors + ". numCPU should not be greater than this.")
  }

  tasksystemlog.info("Master node address is: " + hostConfig.master.toString)

  def printQueueToLog(): Unit = {
    implicit val timeout = akka.util.Timeout(10 seconds)
    import system.dispatcher
    (queueActor ? GetQueueInformation).foreach {
      case queue: QueueInfo => tasksystemlog.info("Queue content: " + queue)
    }
  }

  private val numberOfCores: Int = hostConfig.myCardinality

  private val availableMemory: Int = hostConfig.availableMemory

  private lazy val masterAddress = hostConfig.master

  val reaperActor = elasticSupport.flatMap(_.reaper) match {
    case None =>
      system.actorOf(Props[ProductionReaper], name = "reaper")
    case Some(reaper) => reaper
  }

  val remoteNodeRegistry =
    if (!hostConfig.isApp && hostConfig.isWorker && elasticSupport.isDefined) {
      val remoteActorPath =
        s"akka.tcp://tasks@${masterAddress.getHostName}:${masterAddress.getPort}/user/noderegistry"
      val noderegistry = Await.result(
        system.actorSelection(remoteActorPath).resolveOne(60 seconds),
        atMost = 60 seconds)
      tasksystemlog.info("Remote node registry: " + noderegistry)
      Some(noderegistry)
    } else None

  val remoteFileStorage = new RemoteFileStorage

  val managedFileStorage: Option[ManagedFileStorage] =
    if (config.storageURI.toString == "") None
    else {
      val s3bucket =
        if (config.storageURI.getScheme != null && config.storageURI.getScheme == "s3") {
          Some(
            (config.storageURI.getAuthority, config.storageURI.getPath.drop(1)))
        } else None

      if (s3bucket.isDefined) {
        val actorsystem = 1 // shade implicit conversion
        val _ = actorsystem // suppress unused warning
        Some(new S3Storage(s3bucket.get._1, s3bucket.get._2, s3Stream.get))
      } else {
        val storageFolderPath =
          if (config.storageURI.getScheme == null)
            config.storageURI.getPath
          else if (config.storageURI.getScheme == "file")
            config.storageURI.getPath
          else {
            tasksystemlog.error(
              s"${config.storageURI} unknown protocol, use s3://bucket/key or file:/// (with absolute path), or just a plain path string (absolute or relative")
            throw new RuntimeException(
              s"${config.storageURI} unknown protocol, use s3://bucket/key or file:/// (with absolute path), or just a plain path string (absolute or relative")
          }
        val storageFolder = new File(storageFolderPath).getCanonicalFile
        if (storageFolder.isFile) {
          tasksystemlog.error(s"$storageFolder is a file. Abort.")
          throw new RuntimeException(s"$storageFolder is a file. Abort.")
        }
        if (!storageFolder.isDirectory) {
          tasksystemlog.warning(
            s"Folder $storageFolder does not exists. Try to create it. ")
          storageFolder.mkdirs
        }
        val extendedFolder = config.fileServiceExtendedFolders
        Some(new FolderFileStorage(storageFolder, extendedFolder))
      }
    }

  tasksystemlog.info("File store: " + managedFileStorage)

  val fileActor = try {
    if (hostConfig.isQueue) {

      val threadpoolsize = config.fileServiceThreadPoolSize

      val localFileServiceActor = system.actorOf(
        Props(new FileService(managedFileStorage.get, threadpoolsize))
          .withDispatcher("my-pinned-dispatcher"),
        "fileservice")
      reaperActor ! WatchMe(localFileServiceActor)
      localFileServiceActor
    } else {
      val actorPath =
        s"akka.tcp://tasks@${masterAddress.getHostName}:${masterAddress.getPort}/user/fileservice"
      val remoteFileServieActor = Await.result(
        system.actorSelection(actorPath).resolveOne(600 seconds),
        atMost = 600 seconds)

      remoteFileServieActor
    }
  } catch {
    case e: Throwable => {
      initFailed
      throw e
    }
  }

  tasksystemlog.info("File service actor: " + fileActor)

  val fileServiceComponent =
    FileServiceComponent(fileActor, managedFileStorage, remoteFileStorage)

  val nodeLocalCache = {
    val nodeLocalCacheActor = system.actorOf(
      Props[NodeLocalCache].withDispatcher("my-pinned-dispatcher"),
      name = "nodeLocalCache")

    reaperActor ! WatchMe(nodeLocalCacheActor)

    NodeLocalCacheActor(nodeLocalCacheActor)
  }

  val cacheActor = try {
    if (hostConfig.isQueue) {

      val cache: Cache =
        if (config.cacheEnabled)
          new SharedFileCache()(fileServiceComponent,
                                nodeLocalCache,
                                system,
                                system.dispatcher,
                                config)
        else new DisabledCache

      val localCacheActor = system.actorOf(
        Props(new TaskResultCache(cache, fileServiceComponent))
          .withDispatcher("my-pinned-dispatcher"),
        "cache")
      reaperActor ! WatchMe(localCacheActor)
      localCacheActor
    } else {
      val actorPath =
        s"akka.tcp://tasks@${masterAddress.getHostName}:${masterAddress.getPort}/user/cache"
      Await.result(system.actorSelection(actorPath).resolveOne(600 seconds),
                   atMost = 600 seconds)

    }
  } catch {
    case e: Throwable => {
      initFailed
      throw e
    }
  }

  val queueActor = try {
    if (hostConfig.isQueue) {
      val localActor =
        system.actorOf(Props(new TaskQueue).withDispatcher("taskqueue"),
                       "queue")
      reaperActor ! WatchMe(localActor)
      localActor
    } else {
      val actorPath =
        s"akka.tcp://tasks@${masterAddress.getHostName}:${masterAddress.getPort}/user/queue"
      val remoteActor = Await.result(
        system.actorSelection(actorPath).resolveOne(600 seconds),
        atMost = 600 seconds)

      remoteActor
    }
  } catch {
    case e: Throwable => {
      initFailed
      throw e
    }
  }

  tasksystemlog.info("Queue: " + queueActor)

  val packageServerPort = hostConfig.myAddress.getPort + 1

  val elasticSupportFactory =
    if (hostConfig.isApp || hostConfig.isWorker) {
      val codeAddress =
        if (hostConfig.isApp)
          Some(
            elastic.CodeAddress(
              new java.net.InetSocketAddress(hostConfig.myAddress.getHostName,
                                             packageServerPort),
              config.codeVersion))
        else None

      elasticSupport.map(
        es =>
          es(
            masterAddress = hostConfig.master,
            queueActor = queueActor,
            resource = CPUMemoryAvailable(cpu = hostConfig.myCardinality,
                                          memory = hostConfig.availableMemory),
            codeAddress = codeAddress
        ))
    } else None

  val localNodeRegistry: Option[ActorRef] =
    if (hostConfig.isApp && elasticSupportFactory.isDefined) {

      val props = Props(elasticSupportFactory.get.createRegistry.get)

      val localActor = system
        .actorOf(props.withDispatcher("my-pinned-dispatcher"), "noderegistry")

      reaperActor ! WatchMe(localActor)

      Some(localActor)
    } else None

  val packageServer =
    if (hostConfig.isApp && elasticSupportFactory.isDefined) {
      import akka.http.scaladsl.Http

      Try(Deployment.pack) match {
        case Success(pack) =>
          tasksystemlog
            .info("Written executable package to: {}", pack.getAbsolutePath)

          val service = new PackageServerActor(pack)

          val actorsystem = 1 //shade implicit conversion
          val _ = actorsystem // suppress unused warning
          val bindingFuture =
            Http().bindAndHandle(service.route, "0.0.0.0", packageServerPort)

          Some(bindingFuture)
        case Failure(_) =>
          tasksystemlog.error(
            "Packaging self failed. Main thread exited? Skip starting package server.")
      }

    } else None

  private val auxFjp = tasks.util.concurrent
    .newJavaForkJoinPoolWithNamePrefix("tasks-aux", config.auxThreads)
  private val auxExecutionContext =
    scala.concurrent.ExecutionContext.fromExecutorService(auxFjp)

  val components = TaskSystemComponents(
    queue = QueueActor(queueActor),
    fs = fileServiceComponent,
    actorsystem = system,
    cache = CacheActor(cacheActor),
    nodeLocalCache = nodeLocalCache,
    filePrefix = FileServicePrefix(Vector(), None),
    executionContext = auxExecutionContext,
    actorMaterializer = AM,
    tasksConfig = config
  )

  private val launcherActor = if (numberOfCores > 0 && hostConfig.isWorker) {
    val refreshInterval = config.askInterval
    val localActor = system.actorOf(
      Props(
        new TaskLauncher(
          queueActor,
          nodeLocalCache.actor,
          VersionedCPUMemoryAvailable(
            config.codeVersion,
            CPUMemoryAvailable(cpu = numberOfCores, memory = availableMemory)),
          refreshInterval = refreshInterval,
          auxExecutionContext = auxExecutionContext,
          actorMaterializer = AM,
          remoteStorage = remoteFileStorage,
          managedStorage = managedFileStorage
        ))
        .withDispatcher("launcher"),
      "launcher"
    )
    Some(localActor)
  } else None

  if (!hostConfig.isApp && hostConfig.isWorker && elasticSupportFactory.isDefined && launcherActor.isDefined) {
    val nodeName = getNodeName

    tasksystemlog.info(
      "This is a worker node. ElasticNodeAllocation is enabled. Notifying remote node registry about this node. Node name: " + nodeName)

    remoteNodeRegistry.get ! NodeComingUp(
      Node(RunningJobId(nodeName),
           CPUMemoryAvailable(hostConfig.myCardinality,
                              hostConfig.availableMemory),
           launcherActor.get))

    HeartBeatActor.watch(queueActor)

    system.actorOf(
      Props(elasticSupportFactory.get.createSelfShutdown)
        .withDispatcher("my-pinned-dispatcher"))

  } else {
    tasksystemlog.info("This is not a slave node.")
  }

  private def initFailed(): Unit = {
    if (!hostConfig.isApp && hostConfig.isWorker) {
      tasksystemlog.error(
        "Initialization failed. This is a slave node, notifying remote node registry.")
      remoteNodeRegistry.foreach(_ ! InitFailed(PendingJobId(getNodeName)))
    }
  }

  @volatile
  private var shuttingDown = false

  private def shutdownImpl(): Unit = synchronized {
    if (hostConfig.isApp || hostConfig.isQueue) {
      if (!shuttingDown) {
        shuttingDown = true
        implicit val timeout = akka.util.Timeout(10 seconds)

        val latch = new java.util.concurrent.CountDownLatch(1)
        reaperActor ! Latch(latch)

        if (hostConfig.isQueue) {
          val cacheReaper = system.actorOf(Props(new CallbackReaper({
            fileActor ! PoisonPill
          })))
          (cacheReaper ? WatchMe(cacheActor, answer = true)).foreach { _ =>
            cacheActor ! PoisonPillToCacheActor
          }
          queueActor ! PoisonPill
        }
        localNodeRegistry.foreach(_ ! PoisonPill)
        nodeLocalCache.actor ! PoisonPill

        tasksystemlog.info(
          "Shutting down tasksystem. Blocking until all actors have terminated.")
        latch.await
        auxFjp.shutdown
        AS.terminate
      }
    } else {
      AS.terminate
    }

  }

  val shutdownHook = if (config.addShutdownHook) {
    Some(scala.sys.addShutdownHook {
      tasksystemlog.warning(
        "JVM is shutting down - will call tasksystem shutdown.")
      shutdownImpl
      tasksystemlog.warning(
        "JVM is shutting down - called tasksystem shutdown.")
    })
  } else None

  def shutdown(): Unit = {
    shutdownImpl()
    shutdownHook.foreach(_.remove)
  }

  private def getNodeName: String = elasticSupportFactory.get.getNodeName

}
