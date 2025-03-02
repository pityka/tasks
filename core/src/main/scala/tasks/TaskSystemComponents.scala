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
import tasks.util.message._
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

case class TaskSystemComponents private[tasks] (
    private[tasks] val queue: QueueActor,
    private[tasks] val fs: FileServiceComponent,
    private[tasks] val cache: TaskResultCache,
    private[tasks] val nodeLocalCache: NodeLocalCache.State,
    private[tasks] val filePrefix: FileServicePrefix,
    private[tasks] val tasksConfig: TasksConfig,
    private[tasks] val historyContext: HistoryContext,
    private[tasks] val priority: Priority,
    private[tasks] val labels: Labels,
    private[tasks] val lineage: TaskLineage,
    private[tasks] val messenger: Messenger
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
      hostConfig: Resource[IO, HostConfiguration],
      elasticSupport: Resource[IO, Option[ElasticSupport]],
      s3ClientResource: Resource[IO, Option[tasks.fileservice.s3.S3Client]],
      config: TasksConfig
  ): Resource[IO, (TaskSystemComponents, HostConfiguration)] =
    hostConfig.flatMap { hostConfig =>
      elasticSupport.attempt
        .map {
          case Right(x) => x
          case Left(e) =>
            scribe.error(
              "Failed to create elasticsupport. Continue without it. If this is a worker then self shutdown won't work. If this is a master then spawning nodes won't work.",
              e
            )
            None

        }
        .flatMap { elasticSupport =>
          val masterAddress: tasks.util.SimpleSocketAddress =
            hostConfig.master
          val proxyStoragePort = masterAddress.port + 2

          val packageServerPort = hostConfig.myAddressBind.getPort + 1

          val packageServerHostname = hostConfig.myAddressExternal
            .getOrElse(hostConfig.myAddressBind)
            .getHostName

          val rootHistory = NoHistory

          val s3Client = s3ClientResource.map { s3Client =>
            if (config.storageURI.getScheme == "s3" || config.s3RemoteEnabled) {
              s3Client
            } else None
          }

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
            hostConfig match {
              case _: LocalConfiguration =>
                scribe.info("Remoting disabled.")
              case _ =>
                scribe
                  .info("Listening on: " + hostConfig.myAddressBind.toString)
                scribe.info(
                  "External address: " + hostConfig.myAddressExternal.toString
                )
            }
            scribe.info("CPU: " + hostConfig.availableCPU.toString)
            scribe.info("RAM: " + hostConfig.availableMemory.toString)
            scribe.info("SCRATCH: " + hostConfig.availableScratch.toString)
            scribe
              .info("GPU: " + hostConfig.availableGPU.mkString("[", ", ", "]"))
            scribe.info("Roles: " + hostConfig.myRoles.mkString(", "))
            scribe.info("Elastic: " + elasticSupport)

            if (
              hostConfig.availableCPU > Runtime
                .getRuntime()
                .availableProcessors()
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
                        host =
                          Uri.Host.unsafeFromString(masterAddress.hostName),
                        port = Some(proxyStoragePort)
                      )
                    )
                  )
                )
            }

          val remoteFileStorage = streamHelper
            .map(streamHelper => new RemoteFileStorage()(streamHelper, config))

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
                        .info(
                          s"Started proxy storage server on ${server.baseUri}"
                        )
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
                    new fileservice.s3.S3Storage(
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
                      val storageFolder =
                        new File(storageFolderPath).getCanonicalFile
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

            fileStore
              .flatMap {
                case fs: ManagedFileStorage
                    if config.storageEncryptionKey.isDefined =>
                  Resource.make(
                    IO(
                      new EncryptedManagedFileStorage(
                        fs,
                        config.storageEncryptionKey.get
                      )
                    )
                  )(e => IO(e.destroyKey()))
                case fs: ManagedFileStorage => Resource.pure(fs)

              }
              .flatMap { fileStore =>
                fileStore match {
                  case fs: ManagedFileStorage if config.proxyStorage =>
                    proxyFileStorageHttpServer(fs).map(_ => fs)

                  case fs: ManagedFileStorage => Resource.pure(fs)
                }
              }

          }

          val fileServiceComponent = managedFileStorage
            .flatMap(managedFileStorage =>
              remoteFileStorage.map { remoteFileStorage =>
                scribe.info("File store: " + managedFileStorage) // wrap this
                FileServiceComponent(
                  managedFileStorage,
                  remoteFileStorage
                )
              }
            )

          def cache(fs: FileServiceComponent) = Resource.eval(IO {
            val cache: Cache =
              if (config.cacheEnabled)
                new SharedFileCache()(
                  fs,
                  config
                )
              else new DisabledCache

            new TaskResultCache(cache, fs, config)
          })

          val nodeLocalCache =
            Resource.eval(NodeLocalCache.start.timeout(60 seconds))

          def initFailed(
              remoteNodeRegistry: Option[RemoteNodeRegistry],
              messenger: Messenger
          ) = {
            if (!hostConfig.isApp && hostConfig.isWorker) {
              scribe.error(
                "Initialization failed. This is a follower node, notifying remote node registry."
              )
              messenger.submit(
                Message(
                  MessageData.InitFailed(
                    PendingJobId(elasticSupport.get.getNodeName.getNodeName)
                  ),
                  from = Address("_noaddress_"),
                  to = remoteNodeRegistry.get.address
                )
              )
            } else IO.unit
          }

          case class ActorSet1(
              remoteNodeRegistry: Option[RemoteNodeRegistry]
          )

          def makeQueue(
              cache: TaskResultCache,
              messenger: Messenger,
              remoteNodeRegistry: Option[RemoteNodeRegistry]
          ): Resource[IO, QueueActor] = {

            Resource
              .eval(IO {
                if (hostConfig.isQueue) {
                  QueueActor.makeWithRunloop(cache, messenger)(config)

                } else {

                  Resource.eval(
                    QueueActor
                      .makeReference(
                        masterAddress,
                        messenger,
                        remoteNodeRegistry
                      )(config)
                      .flatMap {
                        case Right(value) => IO.pure(value)
                        case Left(e) =>
                          initFailed(remoteNodeRegistry, messenger) *> IO
                            .raiseError(
                              new RuntimeException("Remote queue failed", e)
                            )
                      }
                  )
                }
              })
              .flatMap(identity)
              .attempt
              .evalMap {
                case Right(value) => IO.pure(value)
                case Left(e) =>
                  initFailed(remoteNodeRegistry, messenger) *> IO.raiseError(
                    new RuntimeException("Remote queue failed", e)
                  )
              }

          }

          def makeRemoteNodeRegistry(
              messenger: Messenger,
              elasticSupport: Option[ElasticSupport]
          ) = {

            if (
              !hostConfig.isApp && hostConfig.isWorker && elasticSupport.isDefined
            ) {
              scribe.info(
                "This is a remote worker node. Looking for remote node registry."
              )
              Resource.eval(
                NodeRegistry
                  .makeReference(masterAddress, messenger, elasticSupport)(
                    config
                  )
                  .map(Some(_))
              )
            } else Resource.pure[IO, Option[RemoteNodeRegistry]](None)

          }

          def elasticSupportFactory(
              queueActor: QueueActor,
              messenger: Messenger
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
                    queueActor = queueActor,
                    resource = ResourceAvailable(
                      cpu = hostConfig.availableCPU,
                      memory = hostConfig.availableMemory,
                      scratch = hostConfig.availableScratch,
                      gpu = hostConfig.availableGPU,
                      image = hostConfig.image
                    ),
                    codeAddress = codeAddress,
                    messenger = messenger
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
                      .info(
                        "Written executable package to: ",
                        pack.getAbsolutePath
                      )

                    val service = new PackageServer(pack)

                    val actorsystem = 1 // shade implicit conversion
                    val _ = actorsystem // suppress unused warning
                    import com.comcast.ip4s._

                    val server = EmberServerBuilder
                      .default[IO]
                      .withHost(ipv4"0.0.0.0")
                      .withPort(
                        com.comcast.ip4s.Port.fromInt(packageServerPort).get
                      )
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
              messenger: Messenger
          ): Resource[IO, Unit] =
            if (hostConfig.isApp && elasticSupportFactory.isDefined) {

              tasks.util.Actor.makeFromBehavior(
                elasticSupportFactory.get.createRegistry.get,
                messenger
              )

            } else Resource.unit

          def launcherActor(
              queueActor: QueueActor,
              nodeLocalCache: NodeLocalCache.State,
              fs: FileServiceComponent,
              cache: TaskResultCache,
              messenger: Messenger
          ) =
            if (hostConfig.availableCPU > 0 && hostConfig.isWorker) {
              val refreshInterval = config.askInterval
              Launcher
                .makeHandle(
                  queueActor,
                  nodeLocalCache,
                  VersionedResourceAvailable(
                    config.codeVersion,
                    ResourceAvailable(
                      cpu = hostConfig.availableCPU,
                      memory = hostConfig.availableMemory,
                      scratch = hostConfig.availableScratch,
                      gpu = hostConfig.availableGPU,
                      image = hostConfig.image
                    )
                  ),
                  refreshInterval = refreshInterval,
                  remoteStorage = fs.remote,
                  managedStorage = fs.storage,
                  cache = cache,
                  messenger = messenger,
                  address = Address(
                    s"Launcher-${hostConfig.myAddressExternal.getOrElse(hostConfig.myAddressBind).toString}"
                  )
                )(config)
                .map(Some(_))

            } else Resource.pure[IO, Option[LauncherHandle]](None)

          def components(
              queueActor: QueueActor,
              fileServiceComponent: FileServiceComponent,
              cache: TaskResultCache,
              nodeLocalCache: NodeLocalCache.State,
              messenger: Messenger
          ) = TaskSystemComponents(
            queue = queueActor,
            fs = fileServiceComponent,
            cache = cache,
            nodeLocalCache = nodeLocalCache,
            filePrefix = FileServicePrefix(Vector()),
            tasksConfig = config,
            historyContext = rootHistory,
            priority = Priority(0),
            labels = Labels.empty,
            lineage = TaskLineage.root,
            messenger = messenger
          )

          def notifyRegistry(
              elasticSupportFactory: Option[ElasticSupport#Inner],
              launcherActor: Option[LauncherActor],
              remoteNodeRegistry: Option[RemoteNodeRegistry],
              messenger: Messenger
          ): Resource[IO, Unit] = {
            val delayed: IO[Resource[IO, Unit]] = IO {
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
                  Resource.eval(initFailed(remoteNodeRegistry, messenger))
                } else {

                  Resource
                    .eval(
                      messenger.submit(
                        Message(
                          MessageData.NodeComingUp(
                            Node(
                              RunningJobId(nodeName),
                              ResourceAvailable(
                                hostConfig.availableCPU,
                                hostConfig.availableMemory,
                                hostConfig.availableScratch,
                                hostConfig.availableGPU,
                                hostConfig.image
                              ),
                              LauncherActor(
                                launcherActor.get.address
                                  .withAddress(messenger.listeningAddress)
                              )
                            )
                          ),
                          from = Address("_noaddress_"),
                          to = remoteNodeRegistry.get.address
                        )
                      )
                    )
                    .flatMap(_ => elasticSupportFactory.get.createSelfShutdown)
                }

              } else {
                scribe.info("This is not a follower node.")
                Resource.unit[IO]
              }
            }

            Resource.eval(delayed).flatMap(identity)
          }

          // def awaitReaper(reaperActor: ActorRef) = IO.interruptible {
          //   val latch = new java.util.concurrent.CountDownLatch(1)
          //   reaperActor ! Latch(latch)
          //   scribe.info(
          //     "Shutting down tasksystem. Blocking until all watched actors have terminated."
          //   )
          //   latch.await
          // }

          for {
            _ <- Resource.make(
              IO(scribe.debug("Start allocation of TaskSystem"))
            )(_ => IO(scribe.debug("Finished deallocation of TaskSystem")))
            _ <- emitLog
            messenger <- Messenger.make(hostConfig)
            remoteNodeRegistry <- makeRemoteNodeRegistry(
              messenger,
              elasticSupport
            )
            fileServiceComponent <- fileServiceComponent
            cache <- cache(fileServiceComponent)
            _ <- Resource.eval(IO(scribe.info(s"Cache: $cache")))
            queue <- makeQueue(
              cache = cache,
              messenger = messenger,
              remoteNodeRegistry = remoteNodeRegistry
            )

            elasticSupportFactory <- elasticSupportFactory(
              queue,
              messenger
            )
            _ <- packageServer(
              elasticSupportFactory
            )
            localNodeRegistry <- localNodeRegistry(
              elasticSupportFactory,
              messenger
            )
            nodeLocalCache <- nodeLocalCache
            launcherHandle <- launcherActor(
              queueActor = queue,
              nodeLocalCache = nodeLocalCache,
              fs = fileServiceComponent,
              cache = cache,
              messenger = messenger
            )
            _ <- notifyRegistry(
              elasticSupportFactory,
              launcherHandle.map(_.launcherActor),
              remoteNodeRegistry,
              messenger
            )
            _ <- Resource.make(
              IO(scribe.debug("Finished allocation of TaskSystem"))
            )(_ => IO(scribe.debug("Start deallocation of TaskSystem")))

          } yield (
            components(
              queueActor = queue,
              fileServiceComponent = fileServiceComponent,
              cache = cache,
              nodeLocalCache = nodeLocalCache,
              messenger = messenger
            ),
            hostConfig
          )

        }
    }
}
