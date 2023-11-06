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

import java.io.File

import scala.concurrent.Await

import scala.concurrent.duration._
import scala.util._
import cats.effect.unsafe.implicits.global
import org.http4s.ember.client.EmberClientBuilder
import cats.effect.IO
import org.http4s.ember.server.EmberServerBuilder
import tasks.fileservice.proxy.ProxyFileStorage

case class TaskSystemComponents(
    queue: QueueActor,
    fs: FileServiceComponent,
    actorsystem: ActorSystem,
    cache: CacheActor,
    nodeLocalCache: NodeLocalCache.State,
    filePrefix: FileServicePrefix,
    tasksConfig: TasksConfig,
    historyContext: HistoryContext,
    priority: Priority,
    labels: Labels,
    lineage: TaskLineage
) {

  def withChildPrefix(name: String) =
    this.copy(filePrefix = this.filePrefix.append(name))

  def withChildPrefix(names: Seq[String]) =
    this.copy(filePrefix = this.filePrefix.append(names))

  def withFilePrefix[B](
      prefix: Seq[String]
  )(fun: TaskSystemComponents => B): B =
    fun(this.withChildPrefix(prefix))
}

class TaskSystem private[tasks] (
    val hostConfig: HostConfiguration,
    val system: ActorSystem,
    val elasticSupport: Option[ElasticSupport]
)(implicit val config: TasksConfig) {

  implicit val AS: ActorSystem = system

  val s3Client =
    if (config.storageURI.getScheme == "s3" || config.s3RemoteEnabled) {
      val s3AWSSDKClient =
        tasks.fileservice.s3.S3.makeAWSSDKClient(config.s3RegionProfileName)
      val s3Client = {
        new tasks.fileservice.s3.S3(
          s3AWSSDKClient
        )
      }
      Some(s3Client -> s3AWSSDKClient)
    } else None

  val httpClientAndRelease = if (config.httpRemoteEnabled) {
    val (httpClient, releaseHttpClient) = EmberClientBuilder
      .default[IO]
      .build
      .allocated
      .unsafeRunSync()
    Some((httpClient, releaseHttpClient))
  } else None

  implicit val streamHelper: StreamHelper =
    new StreamHelper(s3Client.map(_._1), httpClientAndRelease.map(_._1))

  private val tasksystemlog = akka.event.Logging(AS.eventStream, "tasks.boot")

  tasksystemlog.info("Listening on: " + hostConfig.myAddress.toString)
  tasksystemlog.info("CPU: " + hostConfig.availableCPU.toString)
  tasksystemlog.info("RAM: " + hostConfig.availableMemory.toString)
  tasksystemlog.info("SCRATCH: " + hostConfig.availableScratch.toString)
  tasksystemlog.info("GPU: " + hostConfig.availableGPU.mkString("[", ", ", "]"))
  tasksystemlog.info("Roles: " + hostConfig.myRoles.mkString(", "))
  tasksystemlog.info("Elastic: " + elasticSupport)

  if (hostConfig.availableCPU > Runtime.getRuntime().availableProcessors()) {
    tasksystemlog.warning(
      "Number of CPUs in the machine is " + Runtime
        .getRuntime()
        .availableProcessors + ". numCPU should not be greater than this."
    )
  }

  tasksystemlog.info("Master node address is: " + hostConfig.master.toString)

  private lazy val masterAddress: tasks.util.SimpleSocketAddress =
    hostConfig.master

  val reaperActor = elasticSupport.flatMap(_.reaperFactory.map(_.apply)) match {
    case None =>
      system.actorOf(Props[ShutdownActorSystemReaper](), name = "reaper")
    case Some(reaper) => reaper
  }

  val remoteNodeRegistry =
    if (!hostConfig.isApp && hostConfig.isWorker && elasticSupport.isDefined) {
      val remoteActorPath =
        s"akka://tasks@${masterAddress.getHostName}:${masterAddress.getPort}/user/noderegistry"
      val noderegistry = Try(
        Await.result(
          system.actorSelection(remoteActorPath).resolveOne(60 seconds),
          atMost = 60 seconds
        )
      )
      tasksystemlog.info("Remote node registry: " + noderegistry)
      noderegistry match {
        case Success(nr) => Some(nr)
        case Failure(e) =>
          tasksystemlog.error(
            e,
            "Failed to contact remote node registry. Shut down job."
          )
          try {
            elasticSupport.get.selfShutdownNow()
          } finally {
            tasksystemlog.info("Stop jvm")
            System.exit(1)
          }
          None
      }
    } else None

  val remoteFileStorage = new RemoteFileStorage

  val proxyStoragePort = masterAddress.port + 2

  def makeHttpProxyStorageClient(): (ManagedFileStorage, IO[Unit]) = {
    tasksystemlog.info(
      s"Trying to use main application's http proxy storage on address ${masterAddress.hostName} and port ${proxyStoragePort}"
    )
    import org.http4s.Uri
    ProxyFileStorage
      .makeClient(
        uri = org.http4s.Uri(
          scheme = Some(Uri.Scheme.http),
          authority = Some(
            Uri.Authority(
              host = Uri.Host.unsafeFromString(masterAddress.hostName),
              port = Some(proxyStoragePort)
            )
          )
        ),
        as = AS
      )
      .allocated
      .unsafeRunSync()

  }

  def startProxyFileStorageHttpServer(storage: ManagedFileStorage): IO[Unit] = {
    tasksystemlog.info("Starting http server for proxy file storage")
    import com.comcast.ip4s._
    val service = ProxyFileStorage.service(storage)

    val (server, releaseServer) = EmberServerBuilder
      .default[IO]
      .withHost(ipv4"0.0.0.0")
      .withPort(com.comcast.ip4s.Port.fromInt(proxyStoragePort).get)
      .withHttpApp(service.orNotFound)
      .build
      .allocated
      .unsafeRunSync()
    tasksystemlog.info(s"Started proxy storage server on ${server.baseUri}")

    releaseServer
  }

  val (
    managedFileStorage: ManagedFileStorage,
    releaseManagedFileStore: IO[Unit]
  ) = {
    val (fileStore, release1) =
      if (
        (config.storageURI.toString == "" || config.connectToProxyFileServiceOnMain) && !hostConfig.isQueue
      ) {
        makeHttpProxyStorageClient()
      } else {
        val s3bucket =
          if (
            config.storageURI.getScheme != null && config.storageURI.getScheme == "s3"
          ) {
            Some(
              (
                config.storageURI.getAuthority,
                config.storageURI.getPath.drop(1)
              )
            )
          } else None

        if (s3bucket.isDefined) {
          val actorsystem = 1 // shade implicit conversion
          val _ = actorsystem // suppress unused warning

          (
            new s3.S3Storage(
              bucketName = s3bucket.get._1,
              folderPrefix = s3bucket.get._2,
              sse = config.s3ServerSideEncryption,
              cannedAcls = config.s3CannedAcl,
              grantFullControl = config.s3GrantFullControl,
              uploadParallelism = config.s3UploadParallelism,
              s3 = s3Client.get._1
            ),
            IO.unit
          )
        } else {
          val storageFolderPath =
            if (config.storageURI.getScheme == null)
              config.storageURI.getPath
            else if (config.storageURI.getScheme == "file")
              config.storageURI.getPath
            else {
              tasksystemlog.error(
                s"${config.storageURI} unknown protocol, use s3://bucket/key or file:/// (with absolute path), or just a plain path string (absolute or relative"
              )
              throw new RuntimeException(
                s"${config.storageURI} unknown protocol, use s3://bucket/key or file:/// (with absolute path), or just a plain path string (absolute or relative"
              )
            }
          val storageFolder = new File(storageFolderPath).getCanonicalFile
          if (storageFolder.isFile) {
            tasksystemlog.error(s"$storageFolder is a file. Abort.")
            throw new RuntimeException(s"$storageFolder is a file. Abort.")
          }
          if (!storageFolder.isDirectory) {
            if (hostConfig.isQueue) {

              tasksystemlog.warning(
                s"Folder $storageFolder does not exists and this is a master node. Try to create the folder $storageFolder for file storage. "
              )
              storageFolder.mkdirs
              (new FolderFileStorage(storageFolder), IO.unit)
            } else {
              tasksystemlog.warning(
                s"Folder $storageFolder does not exists. This is not a master node. Reverting to proxy via main node."
              )
              makeHttpProxyStorageClient()
            }
          } else {
            (new FolderFileStorage(storageFolder), IO.unit)
          }
        }
      }

    fileStore match {
      case fs: ManagedFileStorage if config.proxyStorage =>
        val release = startProxyFileStorageHttpServer(fs)
        val releaseBoth = release
          .handleError { throwable =>
            tasksystemlog.error(
              "Error in stopping http file storage",
              throwable
            )
          }
          .flatMap(_ =>
            release1.handleError { throwable =>
              tasksystemlog
                .error("Error in stopping http file storage client", throwable)
            }
          )
        (fs, releaseBoth)
      case fs: ManagedFileStorage => (fs, release1)
    }

  }

  tasksystemlog.info("File store: " + managedFileStorage)

  val fileServiceComponent =
    FileServiceComponent(
      // fileActor,
      managedFileStorage,
      remoteFileStorage
    )

  val nodeLocalCache = NodeLocalCache.start.timeout(60 seconds).unsafeRunSync()

  val cacheActor =
    try {
      if (hostConfig.isQueue) {

        val cache: Cache =
          if (config.cacheEnabled)
            new SharedFileCache()(
              fileServiceComponent,
              system,
              config
            )
          else new DisabledCache

        val localCacheActor = system.actorOf(
          Props(new TaskResultCacheActor(cache, fileServiceComponent))
            .withDispatcher("cache-pinned"),
          "cache"
        )
        reaperActor ! WatchMe(localCacheActor)
        localCacheActor
      } else {
        val actorPath =
          s"akka://tasks@${masterAddress.getHostName}:${masterAddress.getPort}/user/cache"
        Await.result(
          system.actorSelection(actorPath).resolveOne(600 seconds),
          atMost = 600 seconds
        )

      }
    } catch {
      case e: Throwable => {
        initFailed()
        throw e
      }
    }

  val uiBootstrap = tasks.ui.UIComponentBootstrap.load

  val trackerBootstrap = tasks.tracker.TrackerBootstrap.load

  val trackerEventListener =
    if (hostConfig.isQueue)
      trackerBootstrap.map(_.start.eventListener)
    else None

  val (queueActor, uiRelease) =
    try {
      if (hostConfig.isQueue) {

        val uiComponent =
          uiBootstrap.map(_.startQueueUI.allocated.unsafeRunSync())
        val uiRelease = uiComponent.map(_._2).getOrElse(IO.unit)

        val eventListeners =
          uiComponent.map(_._1.tasksQueueEventListener).toList ++
            trackerEventListener.toList

        val localActor =
          system.actorOf(
            Props(new TaskQueue(eventListeners))
              .withDispatcher("taskqueue"),
            "queue"
          )
        reaperActor ! WatchMe(localActor)
        (localActor, uiRelease)
      } else {
        val actorPath =
          s"akka://tasks@${masterAddress.getHostName}:${masterAddress.getPort}/user/queue"
        val remoteActor = Await.result(
          system.actorSelection(actorPath).resolveOne(600 seconds),
          atMost = 600 seconds
        )

        (remoteActor, IO.unit)
      }
    } catch {
      case e: Throwable => {
        initFailed()
        throw e
      }
    }

  tasksystemlog.info("Queue: " + queueActor)

  val packageServerPort = hostConfig.myAddress.getPort + 1

  val packageServerHostname = hostConfig.myAddress.getHostName

  val (elasticSupportFactory, nodeUIRelease) =
    if (hostConfig.isApp || hostConfig.isWorker) {

      val uiComponent = if (hostConfig.isApp) {
        Some(uiBootstrap.map(_.startAppUI.allocated.unsafeRunSync()))
      } else None

      val uiRelease = uiComponent.flatMap(_.map(_._2)).getOrElse(IO.unit)

      val codeAddress =
        if (hostConfig.isApp)
          Some(
            elastic.CodeAddress(
              SimpleSocketAddress(
                packageServerHostname,
                packageServerPort
              ),
              config.codeVersion
            )
          )
        else None

      (
        elasticSupport.map(es =>
          es(
            masterAddress = hostConfig.master,
            queueActor = QueueActor(queueActor),
            resource = ResourceAvailable(
              cpu = hostConfig.availableCPU,
              memory = hostConfig.availableMemory,
              scratch = hostConfig.availableScratch,
              gpu = hostConfig.availableGPU
            ),
            codeAddress = codeAddress,
            eventListener =
              uiComponent.flatMap(_.map(_._1.nodeRegistryEventListener))
          )
        ),
        uiRelease
      )
    } else (None, IO.unit)

  val localNodeRegistry: Option[ActorRef] =
    if (hostConfig.isApp && elasticSupportFactory.isDefined) {

      val props = Props(elasticSupportFactory.get.createRegistry.get)

      val localActor = system
        .actorOf(props.withDispatcher("noderegistry-pinned"), "noderegistry")

      reaperActor ! WatchMe(localActor)

      Some(localActor)
    } else None

  val packageServer =
    if (hostConfig.isApp && elasticSupportFactory.isDefined) {

      Try(Deployment.pack) match {
        case Success(pack) =>
          tasksystemlog
            .info("Written executable package to: {}", pack.getAbsolutePath)

          val service = new PackageServer(pack)

          val actorsystem = 1 //shade implicit conversion
          val _ = actorsystem // suppress unused warning
          import com.comcast.ip4s._

          val (server, releaseServer) = EmberServerBuilder
            .default[IO]
            .withHost(ipv4"0.0.0.0")
            .withPort(com.comcast.ip4s.Port.fromInt(packageServerPort).get)
            .withHttpApp(service.route.orNotFound)
            .build
            .allocated
            .unsafeRunSync()

          tasksystemlog.info(s"Started package server on $server")

          Some(releaseServer)
        case Failure(e) =>
          tasksystemlog.error(
            e,
            s"Packaging self failed. Main thread exited? Skip starting package server."
          )
          None
      }

    } else None

  val rootHistory = NoHistory

  val components = TaskSystemComponents(
    queue = QueueActor(queueActor),
    fs = fileServiceComponent,
    actorsystem = system,
    cache = CacheActor(cacheActor),
    nodeLocalCache = nodeLocalCache,
    filePrefix = FileServicePrefix(Vector()),
    tasksConfig = config,
    historyContext = rootHistory,
    priority = Priority(0),
    labels = Labels.empty,
    lineage = TaskLineage.root
  )

  private val launcherActor =
    if (hostConfig.availableCPU > 0 && hostConfig.isWorker) {
      val refreshInterval = config.askInterval
      val localActor = system.actorOf(
        Props(
          new Launcher(
            queueActor,
            nodeLocalCache,
            VersionedResourceAvailable(
              config.codeVersion,
              ResourceAvailable(
                cpu = hostConfig.availableCPU,
                memory = hostConfig.availableMemory,
                scratch = hostConfig.availableScratch,
                gpu = hostConfig.availableGPU
              )
            ),
            refreshInterval = refreshInterval,
            remoteStorage = remoteFileStorage,
            managedStorage = managedFileStorage
          )
        ).withDispatcher("launcher"),
        "launcher"
      )
      Some(localActor)
    } else None

  if (
    !hostConfig.isApp && hostConfig.isWorker && elasticSupportFactory.isDefined && launcherActor.isDefined
  ) {
    tasksystemlog.info("Getting node name..")
    val nodeName = getNodeName

    tasksystemlog.info(
      "This is a worker node. ElasticNodeAllocation is enabled. Notifying remote node registry about this node. Node name: " + nodeName + ". Launcher actor address is: " + launcherActor.get
    )

    val tempFolderWriteable =
      if (!config.checkTempFolderOnSlaveInitialization) true
      else
        Try {
          val testFile = tasks.util.TempFile.createTempFile("test")
          testFile.delete
        }.isSuccess

    if (!tempFolderWriteable) {
      tasksystemlog.error(
        s"Temp folder is not writeable (${System.getProperty("java.io.tmpdir")}). Failing slave init."
      )
      initFailed()
    } else {

      remoteNodeRegistry.get ! NodeComingUp(
        Node(
          RunningJobId(nodeName),
          ResourceAvailable(
            hostConfig.availableCPU,
            hostConfig.availableMemory,
            hostConfig.availableScratch,
            hostConfig.availableGPU
          ),
          launcherActor.get
        )
      )

      system.actorOf(
        Props(elasticSupportFactory.get.createSelfShutdown)
          .withDispatcher("selfshutdown-pinned")
      )
    }

  } else {
    tasksystemlog.info("This is not a slave node.")
  }

  private def initFailed(): Unit = {
    if (!hostConfig.isApp && hostConfig.isWorker) {
      tasksystemlog.error(
        "Initialization failed. This is a slave node, notifying remote node registry."
      )
      remoteNodeRegistry.foreach(_ ! InitFailed(PendingJobId(getNodeName)))
    }
  }

  @volatile
  private var shuttingDown = false

  private def shutdownImpl(): Unit = synchronized {
    if (hostConfig.isApp || hostConfig.isQueue) {
      if (!shuttingDown) {
        shuttingDown = true

        val latch = new java.util.concurrent.CountDownLatch(1)
        reaperActor ! Latch(latch)

        trackerEventListener.foreach(_.close())

        if (hostConfig.isQueue) {

          cacheActor ! PoisonPillToCacheActor
          queueActor ! PoisonPill
        }

        localNodeRegistry.foreach(_ ! PoisonPill)

        tasksystemlog.info(
          "Shutting down tasksystem. Blocking until all watched actors have terminated."
        )
        latch.await
        Await.result(AS.terminate(), 10 seconds)
        s3Client.foreach(_._2.close)
        httpClientAndRelease.foreach(_._2.unsafeRunSync())
        packageServer.foreach(_.unsafeRunSync())
        releaseManagedFileStore.unsafeRunSync()
        uiRelease.unsafeRunSync()
        nodeUIRelease.unsafeRunSync()
      }
    } else {
      s3Client.foreach(_._2.close)
      httpClientAndRelease.foreach(_._2.unsafeRunSync())
      Await.result(AS.terminate(), 10 seconds)
    }

  }

  val shutdownHook = if (config.addShutdownHook) {
    Some(scala.sys.addShutdownHook {
      tasksystemlog.warning(
        "JVM is shutting down - will call tasksystem shutdown."
      )
      shutdownImpl()
      tasksystemlog.warning(
        "JVM is shutting down - called tasksystem shutdown."
      )
    })
  } else None

  def shutdown(): Unit = {
    shutdownImpl()
    shutdownHook.foreach(_.remove())
  }

  private def getNodeName: String = elasticSupportFactory.get.getNodeName

}
