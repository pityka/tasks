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
import cats.effect.kernel.Resource
import org.http4s.client.Client
import tasks.tasksConfig
import org.http4s.server.Server
import com.typesafe.config.ConfigFactory

case class TaskSystemComponents private (
    private[tasks] val queue: QueueActor,
    private[tasks] val fs: FileServiceComponent,
    private[tasks] val actorsystem: ActorSystem,
    private[tasks] val cache: CacheActor,
    private[tasks] val nodeLocalCache: NodeLocalCache.State,
    private[tasks] val filePrefix: FileServicePrefix,
    private[tasks] val tasksConfig: TasksConfig,
    private[tasks] val historyContext: HistoryContext,
    private[tasks] val priority: Priority,
    private[tasks] val labels: Labels,
    private[tasks] val lineage: TaskLineage,
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

object TaskSystemComponents {
  def make(
      hostConfig: HostConfiguration,
      elasticSupport: Option[ElasticSupport],
      config: TasksConfig
  ): Resource[IO, TaskSystemComponents] = {

    val masterAddress: tasks.util.SimpleSocketAddress =
      hostConfig.master
    val proxyStoragePort = masterAddress.port + 2

    val packageServerPort = hostConfig.myAddress.getPort + 1

    val packageServerHostname = hostConfig.myAddress.getHostName

    val rootHistory = NoHistory

    val s3Client =
      Resource.make[IO, Option[tasks.fileservice.s3.S3]](IO {
        if (config.storageURI.getScheme == "s3" || config.s3RemoteEnabled) {
          val s3AWSSDKClient =
            tasks.fileservice.s3.S3
              .makeAWSSDKClient(config.s3RegionProfileName)

          Option(new tasks.fileservice.s3.S3(s3AWSSDKClient))

        } else None
      })(v => IO { v.foreach(_.s3.close) })

    val httpClient =
      if (config.httpRemoteEnabled)
        EmberClientBuilder
          .default[IO]
          .build
          .map(Option(_))
      else Resource.pure[IO, Option[Client[IO]]](None)

    val streamHelper = httpClient.flatMap { http =>
      s3Client.map { s3 =>
        new StreamHelper(s3, http)
      }
    }

    val emitLog = Resource.eval(IO {
      scribe.info("Listening on: " + hostConfig.myAddress.toString)
      scribe.info("CPU: " + hostConfig.availableCPU.toString)
      scribe.info("RAM: " + hostConfig.availableMemory.toString)
      scribe.info("SCRATCH: " + hostConfig.availableScratch.toString)
      scribe.info("GPU: " + hostConfig.availableGPU.mkString("[", ", ", "]"))
      scribe.info("Roles: " + hostConfig.myRoles.mkString(", "))
      scribe.info("Elastic: " + elasticSupport)

      if (
        hostConfig.availableCPU > Runtime.getRuntime().availableProcessors()
      ) {
        scribe.warn(
          "Number of CPUs in the machine is " + Runtime
            .getRuntime()
            .availableProcessors + ". numCPU should not be greater than this."
        )
      }

      scribe.info("Master node address is: " + hostConfig.master.toString)
    })

    val proxyStorageClient: Resource[IO, ManagedFileStorage] = Resource
      .eval(IO {
        scribe.info(
          s"Trying to use main application's http proxy storage on address ${masterAddress.hostName} and port ${proxyStoragePort}"
        )
      })
      .flatMap { _ =>
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
            )
          )
      }

    val remoteFileStorage = streamHelper.map(streamHelper =>
      new RemoteFileStorage()(streamHelper, config)
    )

    def proxyFileStorageHttpServer(storage: ManagedFileStorage) = {
      Resource
        .eval(IO {
          scribe.info("Starting http server for proxy file storage")
        })
        .flatMap { _ =>
          import com.comcast.ip4s._
          EmberServerBuilder
            .default[IO]
            .withHost(ipv4"0.0.0.0")
            .withPort(com.comcast.ip4s.Port.fromInt(proxyStoragePort).get)
            .withHttpApp(ProxyFileStorage.service(storage).orNotFound)
            .build
            .evalTap(server =>
              IO {
                scribe
                  .info(s"Started proxy storage server on ${server.baseUri}")
              }
            )

        }
    }

    val managedFileStorage = {
      val fileStore =
        if (
          (config.storageURI.toString == "" || config.connectToProxyFileServiceOnMain) && !hostConfig.isQueue
        ) {
          proxyStorageClient
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

            s3Client.map(s3Client =>
              new s3.S3Storage(
                bucketName = s3bucket.get._1,
                folderPrefix = s3bucket.get._2,
                sse = config.s3ServerSideEncryption,
                cannedAcls = config.s3CannedAcl,
                grantFullControl = config.s3GrantFullControl,
                uploadParallelism = config.s3UploadParallelism,
                s3 = s3Client.get
              )(config)
            )
          } else {
            Resource
              .eval(IO {
                val storageFolderPath =
                  if (config.storageURI.getScheme == null)
                    config.storageURI.getPath
                  else if (config.storageURI.getScheme == "file")
                    config.storageURI.getPath
                  else {
                    scribe.error(
                      s"${config.storageURI} unknown protocol, use s3://bucket/key or file:/// (with absolute path), or just a plain path string (absolute or relative"
                    )
                    throw new RuntimeException(
                      s"${config.storageURI} unknown protocol, use s3://bucket/key or file:/// (with absolute path), or just a plain path string (absolute or relative"
                    )
                  }
                val storageFolder = new File(storageFolderPath).getCanonicalFile
                if (storageFolder.isFile) {
                  scribe.error(s"$storageFolder is a file. Abort.")
                  throw new RuntimeException(
                    s"$storageFolder is a file. Abort."
                  )
                }
                if (!storageFolder.isDirectory) {
                  if (hostConfig.isQueue) {

                    scribe.warn(
                      s"Folder $storageFolder does not exists and this is a master node. Try to create the folder $storageFolder for file storage. "
                    )
                    storageFolder.mkdirs
                    Resource.pure[IO, ManagedFileStorage](
                      (new FolderFileStorage(storageFolder)(config))
                    )
                  } else {
                    scribe.warn(
                      s"Folder $storageFolder does not exists. This is not a master node. Reverting to proxy via main node."
                    )
                    proxyStorageClient
                  }
                } else {
                  Resource.pure[IO, ManagedFileStorage](
                    new FolderFileStorage(storageFolder)(config)
                  )
                }
              })
              .flatMap(identity)
          }
        }

      fileStore.flatMap { fileStore =>
        fileStore match {
          case fs: ManagedFileStorage if config.proxyStorage =>
            proxyFileStorageHttpServer(fs).map(_ => fs)

          case fs: ManagedFileStorage => Resource.pure(fs)
        }
      }

    }

    val fileServiceComponent = managedFileStorage.flatMap(managedFileStorage =>
      remoteFileStorage.map { remoteFileStorage =>
        scribe.info("File store: " + managedFileStorage) //wrap this
        FileServiceComponent(
          managedFileStorage,
          remoteFileStorage
        )
      }
    )

    val nodeLocalCache = Resource.eval(NodeLocalCache.start.timeout(60 seconds))

    def initFailed(remoteNodeRegistry: Option[ActorRef]): Unit = {
      if (!hostConfig.isApp && hostConfig.isWorker) {
        scribe.error(
          "Initialization failed. This is a slave node, notifying remote node registry."
        )
        remoteNodeRegistry.foreach(
          _ ! InitFailed(PendingJobId(elasticSupport.get.getNodeName.getNodeName))
        )
      }
    }

    case class ActorSet1(
        queueActor: ActorRef,
        cacheActor: ActorRef,
        reaperActor: ActorRef,
        remoteNodeRegistry: Option[ActorRef]
    )

    def makeActors(
        fileServiceComponent: FileServiceComponent,
        system: ActorSystem
    ) = {

      Resource.make {
        IO.interruptible {

          val reaperActor: ActorRef =
            elasticSupport.flatMap(
              _.reaperFactory.map(_.apply(system, config))
            ) match {
              case None =>
                system.actorOf(
                  Props[ShutdownActorSystemReaper](),
                  name = "reaper"
                )
              case Some(reaper) => reaper
            }

          val remoteNodeRegistry: Option[ActorRef] =
            if (
              !hostConfig.isApp && hostConfig.isWorker && elasticSupport.isDefined
            ) {
              scribe.info(
                "This is a remote worker node. Looking for remote node registry."
              )
              val remoteActorPath =
                s"akka://tasks@${masterAddress.getHostName}:${masterAddress.getPort}/user/noderegistry"
              val noderegistry = Try(
                Await.result(
                  system.actorSelection(remoteActorPath).resolveOne(60 seconds),
                  atMost = 60 seconds
                )
              )
              scribe.info("Remote node registry: " + noderegistry)
              noderegistry match {
                case Success(nr) => Some(nr)
                case Failure(e) =>
                  scribe.error(
                    e,
                    "Failed to contact remote node registry. Shut down job."
                  )
                  try {
                    elasticSupport.get.selfShutdownNow()
                  } finally {
                    scribe.info("Stop jvm")
                    System.exit(1)
                  }
                  None
              }
            } else None

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
                  Props(
                    new TaskResultCacheActor(cache, fileServiceComponent)(
                      config
                    )
                  )
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
                initFailed(remoteNodeRegistry)
                throw e
              }
            }

          val queueActor =
            try {
              if (hostConfig.isQueue) {

                val localActor =
                  system.actorOf(
                    Props(new TaskQueue(Nil)(config))
                      .withDispatcher("taskqueue"),
                    "queue"
                  )
                reaperActor ! WatchMe(localActor)
                localActor
              } else {
                val actorPath =
                  s"akka://tasks@${masterAddress.getHostName}:${masterAddress.getPort}/user/queue"
                val remoteActor = Await.result(
                  system.actorSelection(actorPath).resolveOne(600 seconds),
                  atMost = 600 seconds
                )

                remoteActor
              }
            } catch {
              case e: Throwable => {
                initFailed(remoteNodeRegistry)
                throw e
              }
            }

          scribe.info("Queue: " + queueActor)
          ActorSet1(queueActor, cacheActor, reaperActor, remoteNodeRegistry)
        }
      }(actorSet =>
        IO {
          awaitReaper(actorSet.reaperActor)
          if (hostConfig.isQueue) {
            actorSet.cacheActor ! PoisonPillToCacheActor
            actorSet.queueActor ! PoisonPill
          }
        }
      )
    }

    def elasticSupportFactory(
        queueActor: ActorRef
    ): Resource[IO, Option[ElasticSupport#Inner]] =
      if (hostConfig.isApp || hostConfig.isWorker) {

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

        Resource.eval[IO, Option[ElasticSupport#Inner]](IO {
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
              eventListener = None
            )(config)
          )
        })

      } else Resource.pure(None)

    def packageServer(
        elasticSupportFactory: Option[ElasticSupport#Inner]
    ): Resource[IO, Option[Server]] = Resource
      .eval(IO {
        if (hostConfig.isApp && elasticSupportFactory.isDefined) {

          Try(Deployment.pack(config)) match {
            case Success(pack) =>
              scribe
                .info("Written executable package to: {}", pack.getAbsolutePath)

              val service = new PackageServer(pack)

              val actorsystem = 1 //shade implicit conversion
              val _ = actorsystem // suppress unused warning
              import com.comcast.ip4s._

              val server = EmberServerBuilder
                .default[IO]
                .withHost(ipv4"0.0.0.0")
                .withPort(com.comcast.ip4s.Port.fromInt(packageServerPort).get)
                .withHttpApp(service.route.orNotFound)
                .build

              // scribe.info(s"Started package server on $server")

              (server.map(Some(_)): Resource[IO, Option[Server]])
            case Failure(e) =>
              scribe.error(
                e,
                s"Packaging self failed. Main thread exited? Skip starting package server."
              )
              Resource.pure[IO, Option[Server]](Option.empty[Server])
          }

        } else Resource.pure[IO, Option[Server]](Option.empty[Server])
      })
      .flatMap(identity)

    def localNodeRegistry(
        elasticSupportFactory: Option[ElasticSupport#Inner],
        reaperActor: ActorRef,
        system: ActorSystem
    ): Resource[IO, Option[ActorRef]] =
      Resource.make(IO {
        if (hostConfig.isApp && elasticSupportFactory.isDefined) {

          val props = Props(elasticSupportFactory.get.createRegistry.get)

          val localActor = system
            .actorOf(
              props.withDispatcher("noderegistry-pinned"),
              "noderegistry"
            )

          reaperActor ! WatchMe(localActor)

          Some(localActor)
        } else None
      }) { localActor =>
        IO { localActor.foreach(_ ! PoisonPill) }
      }

    def launcherActor(
        queueActor: ActorRef,
        nodeLocalCache: NodeLocalCache.State,
        fs: FileServiceComponent,
        system: ActorSystem
    ) =
      Resource.eval(IO {
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
                remoteStorage = fs.remote,
                managedStorage = fs.storage
              )(config)
            ).withDispatcher("launcher"),
            "launcher"
          )
          Some(localActor)
        } else None
      })

    def components(
        queueActor: ActorRef,
        fileServiceComponent: FileServiceComponent,
        cacheActor: ActorRef,
        nodeLocalCache: NodeLocalCache.State,
        system: ActorSystem
    ) = TaskSystemComponents(
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
      lineage = TaskLineage.root,
    )

    def notifyRegistry(
        elasticSupportFactory: Option[ElasticSupport#Inner],
        launcherActor: Option[ActorRef],
        remoteNodeRegistry: Option[ActorRef],
        system: ActorSystem
    ) =
      Resource.eval(IO {
        if (
          !hostConfig.isApp && hostConfig.isWorker && elasticSupportFactory.isDefined && launcherActor.isDefined
        ) {
          scribe.info("Getting node name..")
          val nodeName = elasticSupportFactory.get.getNodeName

          scribe.info(
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
            scribe.error(
              s"Temp folder is not writeable (${System.getProperty("java.io.tmpdir")}). Failing slave init."
            )
            initFailed(remoteNodeRegistry)
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
          scribe.info("This is not a slave node.")
        }
      })

    def awaitReaper(reaperActor: ActorRef) = IO.interruptible {
      val latch = new java.util.concurrent.CountDownLatch(1)
      reaperActor ! Latch(latch)
      scribe.info(
        "Shutting down tasksystem. Blocking until all watched actors have terminated."
      )
      latch.await
    }

    def makeAS = Resource.make(IO {
      val finalAkkaConfiguration = {

        val actorProvider = hostConfig match {
          case _: LocalConfiguration => "akka.actor.LocalActorRefProvider"
          case _                     => "akka.remote.RemoteActorRefProvider"
        }

        val akkaProgrammaticalConfiguration = ConfigFactory.parseString(s"""
        task-worker-dispatcher.fork-join-executor.parallelism-max = ${hostConfig.availableCPU}
        task-worker-dispatcher.fork-join-executor.parallelism-min = ${hostConfig.availableCPU}
        
        akka {
          actor {
            provider = "${actorProvider}"
          }
          remote {
            artery {
              canonical.hostname = "${hostConfig.myAddress.getHostName}"
              canonical.port = ${hostConfig.myAddress.getPort.toString}
            }
            
         }
        }
          """)

        ConfigFactory.defaultOverrides
          .withFallback(akkaProgrammaticalConfiguration)
          .withFallback(ConfigFactory.parseResources("akka.conf"))
          .withFallback(ConfigFactory.load)

      }

      ActorSystem(config.actorSystemName, finalAkkaConfiguration)
    })(as =>
      IO.fromFuture {
        IO(as.terminate())
      }.void
    )

    for {
      _ <- emitLog
      as <- makeAS
      fileServiceComponent <- fileServiceComponent
      nodeLocalCache <- nodeLocalCache
      actorSet <- makeActors(fileServiceComponent, as)

      elasticSupportFactory <- elasticSupportFactory(
        actorSet.queueActor
      )
      _ <- packageServer(
        elasticSupportFactory
      )
      localNodeRegistry <- localNodeRegistry(
        elasticSupportFactory,
        actorSet.reaperActor,
        as
      )
      launcherActor <- launcherActor(
        actorSet.queueActor,
        nodeLocalCache,
        fileServiceComponent,
        as
      )
      _ <- notifyRegistry(
        elasticSupportFactory,
        launcherActor,
        actorSet.remoteNodeRegistry,
        as
      )

    } yield components(
      actorSet.queueActor,
      fileServiceComponent,
      actorSet.cacheActor,
      nodeLocalCache,
      as
    )

  }
}
