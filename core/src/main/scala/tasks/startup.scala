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
import tasks.elastic.ec2._
import tasks.shared._

import akka.actor._
import akka.pattern.ask
import akka.stream._

import java.net.InetSocketAddress
import java.io.File

import scala.concurrent.Await

import scala.concurrent.duration._
import scala.concurrent._

import com.bluelabs.s3stream.S3ClientQueued

case class TaskSystemComponents(
    queue: QueueActor,
    fs: FileServiceActor,
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
    val hostConfig: MasterSlaveConfiguration,
    val system: ActorSystem)(implicit val config: TasksConfig) {

  implicit val as = system
  implicit val mat = ActorMaterializer()
  import as.dispatcher
  implicit val s3Stream = scala.util
    .Try(new S3ClientQueued(config.s3Region))
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

  def printQueueToLog(): Unit = {
    implicit val timeout = akka.util.Timeout(10 seconds)
    import system.dispatcher
    (queueActor ? GetQueueInformation).foreach {
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
      system.actorOf(Props(new EC2Reaper(config.terminateMaster)),
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
        import as.dispatcher
        Some(new S3Storage(s3bucket.get._1, s3bucket.get._2, s3Stream.get))
      } else {
        val folder1Path =
          if (config.storageURI.getScheme == null)
            config.storageURI.getPath
          else if (config.storageURI.getScheme == "file")
            config.storageURI.getPath
          else {
            tasksystemlog.error(
              s"${config.storageURI} unknown protocol, use s3://bucket/key or file:/// (with absolute path), or just a plain path string (absolute or relative")
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
        val folders2 = config.fileServiceExtendedFolders
        if (folder1.list.size != 0) {
          tasksystemlog.warning(
            s"fileServiceBaseFolder (${folder1.getCanonicalPath}) is not empty. This is only safe if you restart a pipeline. ")
        }
        Some(new FolderFileStorage(folder1, folders2))
      }
    }

  tasksystemlog.info("FileStore: " + managedFileStorage)

  val fileActor = try {
    if (!isLauncherOnly) {

      val threadpoolsize = config.fileServiceThreadPoolSize

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
    if (!isLauncherOnly) {

      val cache: Cache =
        if (config.cacheEnabled)
          new SharedFileCache()(fileServiceActor,
                                nodeLocalCache,
                                system,
                                system.dispatcher,
                                config)
        else new DisabledCache

      val ac = system.actorOf(
        Props(new TaskResultCache(cache, fileServiceActor))
          .withDispatcher("my-pinned-dispatcher"),
        "cache")
      reaperActor ! WatchMe(ac)
      ac
    } else {
      val actorpath =
        s"akka.tcp://tasks@${masterAddress.getHostName}:${masterAddress.getPort}/user/cache"
      Await.result(system.actorSelection(actorpath).resolveOne(600 seconds),
                   atMost = 600 seconds)

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
        system.actorOf(Props(new TaskQueue).withDispatcher("taskqueue"),
                       "queue")
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

  val packageServer =
    if (!isLauncherOnly && elasticSupportFactory.isDefined) {
      import akka.http.scaladsl.Http

      val pack = Deployment.pack
      tasksystemlog
        .info("Written executable package to: {}", pack.getAbsolutePath)

      val service = new PackageServerActor(pack)

      val actorsystem = 1 //shade implicit conversion
      val _ = actorsystem // suppress unused warning
      val bindingFuture =
        Http().bindAndHandle(service.route, "0.0.0.0", host.getPort + 1)

      Some(bindingFuture)

    } else None

  private val auxFjp = tasks.util.concurrent
    .newJavaForkJoinPoolWithNamePrefix("tasks-aux", config.auxThreads)
  private val auxExecutionContext =
    scala.concurrent.ExecutionContext.fromExecutorService(auxFjp)

  val components = TaskSystemComponents(
    queue = QueueActor(queueActor),
    fs = fileServiceActor,
    actorsystem = system,
    cache = CacheActor(cacherActor),
    nodeLocalCache = nodeLocalCache,
    filePrefix = FileServicePrefix(Vector()),
    executionContext = auxExecutionContext,
    actorMaterializer = mat,
    tasksConfig = config
  )

  private val launcherActor = if (numberOfCores > 0) {
    val refresh = config.askInterval
    val ac = system.actorOf(
      Props(
        new TaskLauncher(
          queueActor,
          nodeLocalCacheActor,
          CPUMemoryAvailable(cpu = numberOfCores, memory = availableMemory),
          refreshRate = refresh,
          auxExecutionContext = auxExecutionContext,
          actorMaterializer = mat,
          remoteStorage = remoteFileStorage,
          managedStorage = managedFileStorage
        ))
        .withDispatcher("launcher"),
      "launcher"
    )
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

    system.actorOf(
      Props(new HeartBeatActor(queueActor)).withDispatcher("heartbeat"),
      "heartbeatOf" + queueActor.path.address.toString
        .replace("://", "___") + queueActor.path.name
    )

    system.actorOf(
      Props(elasticSupportFactory.get.createSelfShutdown)
        .withDispatcher("my-pinned-dispatcher"))

  } else {
    tasksystemlog.warning("Nodename/jobname is not defined.")
  }

  private def initFailed(): Unit = {
    if (isLauncherOnly) {
      remotenoderegistry.foreach(_ ! InitFailed(PendingJobId(getNodeName)))
    }
  }

  var shuttingDown = false

  def shutdown(): Unit = synchronized {
    if (hostConfig.myRole == MASTER) {
      if (!shuttingDown) {
        shuttingDown = true
        implicit val timeout = akka.util.Timeout(10 seconds)
        import system.dispatcher

        val latch = new java.util.concurrent.CountDownLatch(1)
        reaperActor ! Latch(latch)
        val cacheReaper = system.actorOf(Props(new CallbackReaper({
          fileActor ! PoisonPill
        })))
        (cacheReaper ? WatchMe(cacherActor)).foreach { _ =>
          cacherActor ! PoisonPillToCacheActor
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
      system.terminate
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
