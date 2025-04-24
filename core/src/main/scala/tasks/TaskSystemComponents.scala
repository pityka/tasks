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
import tasks.queue.Launcher.LauncherHandle
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
import org.ekrich.config.ConfigFactory
import cats.effect.kernel.Deferred
import cats.effect.ExitCode

case class TaskSystemComponents private[tasks] (
    private[tasks] val queue: Queue,
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
  private[tasks] def make(
      hostConfig: Resource[IO, HostConfiguration],
      elasticSupport: Resource[IO, Option[ElasticSupport]],
      s3ClientResource: Resource[IO, Option[tasks.fileservice.s3.S3Client]],
      externalQueueState: Resource[IO, Option[Transaction[QueueImpl.State]]],
      config: TasksConfig,
      exitCode: Deferred[IO, ExitCode]
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
          // val masterAddress: tasks.util.SimpleSocketAddress =
          //   hostConfig.master
          def proxyStoragePort(t: RemotingHostConfiguration) = t.master.port + 2

          def packageServerPort(t: RemotingHostConfiguration) =
            t.myAddressBind.getPort + 1

          def packageServerHostname(t: RemotingHostConfiguration) =
            t.myAddressExternal
              .getOrElse(t.myAddressBind)
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
              case t: RemotingHostConfiguration =>
                scribe
                  .info(
                    "Listening on: " + t.myAddressBind.toString + s" prefix: ${t.bindPrefix}"
                  )
                scribe.info(
                  "External address: " + t.myAddressExternal.toString
                )
                scribe.info(
                  "Master node address is: " + t.master.toString + s" prefix: ${t.masterPrefix}"
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

          })

          def proxyStorageClient(
              t: RemotingHostConfiguration
          ): Resource[IO, ManagedFileStorage] =
            Resource
              .eval(IO {
                scribe.info(
                  s"Trying to use main application's http proxy storage on address ${t.master.hostName} and port ${proxyStoragePort(t)}"
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
                          host = Uri.Host.unsafeFromString(t.master.hostName),
                          port = Some(proxyStoragePort(t))
                        )
                      )
                    )
                  )

              }

          val remoteFileStorage = streamHelper
            .map(streamHelper => new RemoteFileStorage()(streamHelper, config))

          def proxyFileStorageHttpServer(
              t: RemotingHostConfiguration,
              storage: ManagedFileStorage
          ) = {
            Resource
              .eval(IO {
                scribe.info("Starting http server for proxy file storage")
              })
              .flatMap { _ =>
                import com.comcast.ip4s._
                EmberServerBuilder
                  .default[IO]
                  .withHost(ipv4"0.0.0.0")
                  .withPort(
                    com.comcast.ip4s.Port.fromInt(proxyStoragePort(t)).get
                  )
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
            val fileStore: Resource[IO, ManagedFileStorage] =
              if (
                (config.storageURI.toString == "" || config.connectToProxyFileServiceOnMain) && !hostConfig.isQueue && hostConfig
                  .isInstanceOf[RemotingHostConfiguration]
              ) {
                proxyStorageClient(
                  hostConfig.asInstanceOf[RemotingHostConfiguration]
                )
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
                    (new fileservice.s3.S3Storage(
                      bucketName = s3bucket.get._1,
                      folderPrefix = s3bucket.get._2,
                      sse = config.s3ServerSideEncryption,
                      cannedAcls = config.s3CannedAcl,
                      grantFullControl = config.s3GrantFullControl,
                      uploadParallelism = config.s3UploadParallelism,
                      s3 = s3Client.get
                    )(config))
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
                          hostConfig match {
                            case t: RemotingHostConfiguration =>
                              proxyStorageClient(t)
                            case _ => ???
                          }

                        }
                      } else {
                        Resource.pure[IO, ManagedFileStorage](
                          (new FolderFileStorage(storageFolder)(config))
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
                    hostConfig match {
                      case t: RemotingHostConfiguration =>
                        proxyFileStorageHttpServer(t, fs).map(_ => fs)
                      case _ => Resource.pure(fs)
                    }

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

          def makeQueue(
              cache: TaskResultCache,
              messenger: Messenger,
              externalQueueState: Option[Transaction[QueueImpl.State]],
              shutdownNode: Option[tasks.elastic.ShutdownNode],
              decideNewNode: Option[tasks.elastic.DecideNewNode],
              createNode: Option[tasks.elastic.CreateNode],
              unmanagedResource: tasks.shared.ResourceAvailable,
              shutdownSelf: Option[tasks.elastic.ShutdownSelfNode],
              exitCode: Deferred[IO, ExitCode],
              nodeName: Option[RunningJobId]
          ): Resource[IO, Queue] = {

            val io: IO[Resource[IO, Queue]] = IO {
              if (externalQueueState.isDefined) {
                scribe.debug(
                  s"Using direct connection to external queue: ${externalQueueState.get}"
                )

                QueueImpl
                  .fromTransaction(
                    externalQueueState.get,
                    cache,
                    messenger,
                    shutdownNode,
                    decideNewNode,
                    createNode,
                    unmanagedResource
                  )(config)
                  .map { queueImpl =>
                    (new QueueFromQueueImpl(
                      queueImpl
                    ): Queue)
                  }

              } else if (hostConfig.isQueue) {
                scribe.debug(
                  s"Using in memory proxied queue state. Spawning central queue state."
                )

                QueueImpl
                  .initRef(
                    cache,
                    messenger,
                    shutdownNode,
                    decideNewNode,
                    createNode,
                    unmanagedResource
                  )(config)
                  .flatMap { impl =>
                    QueueActor
                      .makeWithQueueImpl(impl, cache, messenger)(config)
                      .map { _ =>
                        // queue actor is to serve as a remote endpoint
                        // for local access use directly the impl
                        (new QueueFromQueueImpl(impl): Queue)
                      }
                  }

              } else {
                scribe.info(
                  s"Using in memory proxied queue state from remote queue."
                )
                Resource.eval(
                  QueueActor
                    .makeReference(
                      messenger
                    )(config)
                    .flatMap {
                      case Right(value) =>
                        IO {
                          scribe.info(s"Got remote queue: $value")
                          (new tasks.queue.QueueWithActor(
                            value,
                            messenger
                          ): Queue)
                        }
                      case Left(e) =>
                        nodeName
                          .flatMap(n =>
                            shutdownSelf.map(_.shutdownRunningNode(exitCode, n))
                          )
                          .getOrElse(IO.unit) *> IO
                          .raiseError(
                            new RuntimeException("Remote queue failed", e)
                          )
                    }
                )
              }
            }.attempt
              .flatMap {
                case Right(value) => IO.pure(value)
                case Left(e) =>
                  nodeName
                    .flatMap(n =>
                      shutdownSelf.map(_.shutdownRunningNode(exitCode, n))
                    )
                    .getOrElse(IO.unit) *> IO.raiseError(
                    new RuntimeException("Remote queue failed", e)
                  )
              }

            Resource.eval(io).flatMap(identity)

          }

          val codeAddress =
            if (hostConfig.isApp)
              hostConfig match {
                case t: RemotingHostConfiguration =>
                  Some(
                    elastic.CodeAddress(
                      SimpleSocketAddress(
                        packageServerHostname(t),
                        packageServerPort(t)
                      ),
                      config.codeVersion
                    )
                  )
                case _ => None
              }
            else None

          def makeCodeAddress(
              t: RemotingHostConfiguration,
              server: Option[Server]
          ): Resource[IO, Option[CodeAddress]] =
            Resource.eval(IO.pure(server.map { _ =>
              (
                elastic.CodeAddress(
                  SimpleSocketAddress(
                    packageServerHostname(t),
                    packageServerPort(t)
                  ),
                  config.codeVersion
                )
              )

            }))

          def makePackageServer(
              t: RemotingHostConfiguration,
              enable: Boolean
          ): Resource[IO, Option[Server]] = Resource
            .eval(IO {
              if (hostConfig.isApp && enable) {

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
                        com.comcast.ip4s.Port.fromInt(packageServerPort(t)).get
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

          def initFailed(
              queue: Queue
          ) = {
            (if (!hostConfig.isApp && hostConfig.isWorker) {
               scribe.error(
                 "Initialization failed. This is a follower node, notifying remote node registry."
               )
               elasticSupport.get.getNodeName.getNodeName(config).flatMap {
                 nodeName =>
                   queue.initFailed(nodeName)
               }
             } else
               IO.unit) *> IO.raiseError(new RuntimeException("init failed"))
          }

          def tempFolderIsWriteable(queue: Queue) = {
            val tempFolderWriteable =
              if (!config.checkTempFolderOnWorkerInitialization) true
              else
                Try {
                  val testFile =
                    tasks.util.TempFile.createTempFile("test")
                  testFile.delete
                }.isSuccess

            if (!tempFolderWriteable) {
              scribe.error(
                s"Temp folder is not writeable (${System.getProperty("java.io.tmpdir")}). Failing worker init."
              )
              Resource.eval(initFailed(queue))
            } else Resource.pure[IO, Unit](())
          }

          def launcherActor(
              queue: Queue,
              nodeLocalCache: NodeLocalCache.State,
              fs: FileServiceComponent,
              cache: TaskResultCache,
              messenger: Messenger,
              node: Option[Node],
              shutdown: Option[tasks.elastic.ShutdownSelfNode],
              exitCode: Option[Deferred[IO, ExitCode]],
              launcherName: LauncherName
          ) =
            if (hostConfig.availableCPU > 0 && hostConfig.isWorker) {
              val refreshInterval = config.askInterval
              Launcher
                .makeHandle(
                  queue,
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
                  address = launcherName,
                  node = node,
                  shutdown = shutdown,
                  exitCode = exitCode
                )(config)
                .map(Some(_))

            } else Resource.pure[IO, Option[LauncherHandle]](None)

          def components(
              queue: Queue,
              fileServiceComponent: FileServiceComponent,
              cache: TaskResultCache,
              nodeLocalCache: NodeLocalCache.State,
              messenger: Messenger
          ) = TaskSystemComponents(
            queue = queue,
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

          def getNodeName(
          ) = Resource.eval(
            elasticSupport
              .map(
                _.getNodeName
                  .getNodeName(config)
                  .map(s => Some(s))
              )
              .getOrElse(IO.pure(None))
          )

          def makeLauncherName() =
            Resource.pure[IO, LauncherName](
              LauncherName(
                hostConfig match {
                  case t: RemotingHostConfiguration =>
                    s"Launcher-${t.myAddressExternal.getOrElse(t.myAddressBind).toString}"
                  case _ => s"Launcher"
                }
              )
            )

          def makeNode(
              launcherName: LauncherName,
              nodeName: Option[RunningJobId]
          ): Resource[IO, Option[Node]] = {
            if (
              !hostConfig.isApp && hostConfig.isWorker && nodeName.isDefined
            ) {
              scribe.info(
                "This is a worker node. ElasticNodeAllocation is enabled. Node name: " + nodeName.get + ". Launcher actor address is: " + launcherName
              )

              Resource.pure(
                Option(
                  Node(
                    nodeName.get,
                    ResourceAvailable(
                      hostConfig.availableCPU,
                      hostConfig.availableMemory,
                      hostConfig.availableScratch,
                      hostConfig.availableGPU,
                      hostConfig.image
                    ),
                    launcherName
                  )
                )
              )

            } else {
              scribe.info("This is not a follower node.")
              Resource.unit[IO].map(_ => None)
            }

          }

          for {
            _ <- Resource.make(
              IO(scribe.debug("Start allocation of TaskSystem"))
            )(_ => IO(scribe.debug("Finished deallocation of TaskSystem")))
            _ <- emitLog
            nodeLocalCache <- nodeLocalCache
            codeAddress <- {
              hostConfig match {
                case t: RemotingHostConfiguration =>
                  makePackageServer(t, elasticSupport.isDefined)
                    .flatMap(x => makeCodeAddress(t, x))
                case _ => Resource.eval(IO.pure(None))
              }
            }
            nodeName <- getNodeName()
            launcherName <- makeLauncherName()
            node <- makeNode(
              launcherName = launcherName,
              nodeName = nodeName
            )
            messenger <- Messenger.make(hostConfig)
            fileServiceComponent <- fileServiceComponent
            cache <- cache(fileServiceComponent)
            _ <- Resource.eval(IO(scribe.info(s"Cache: $cache")))
            externalQueueState <- externalQueueState
            queue <- makeQueue(
              cache = cache,
              messenger = messenger,
              externalQueueState = externalQueueState,
              shutdownNode = elasticSupport.map(_.shutdownFromNodeRegistry),
              decideNewNode = codeAddress.map(codeAddress =>
                new SimpleDecideNewNode(codeAddress.codeVersion)(config)
              ),
              createNode = codeAddress.flatMap(codeAddress =>
                hostConfig match {
                  case t: RemotingHostConfiguration =>
                    elasticSupport.map(
                      _.createNodeFactory.apply(
                        masterAddress = t.master,
                        masterPrefix = t.masterPrefix,
                        codeAddress = codeAddress
                      )
                    )
                  case _ => None
                }
              ),
              unmanagedResource = ResourceAvailable.empty,
              shutdownSelf = elasticSupport.map(_.shutdownFromWorker),
              exitCode = exitCode,
              nodeName = nodeName
            )
            _ <- tempFolderIsWriteable(queue)
            launcherHandle <- launcherActor(
              queue = queue,
              nodeLocalCache = nodeLocalCache,
              fs = fileServiceComponent,
              cache = cache,
              messenger = messenger,
              launcherName = launcherName,
              node = node,
              shutdown = elasticSupport.map(_.shutdownFromWorker),
              exitCode = elasticSupport.map(_ => exitCode)
            )

            _ <- Resource.make(
              IO(scribe.debug("Finished allocation of TaskSystem"))
            )(_ => IO(scribe.debug("Start deallocation of TaskSystem")))

          } yield (
            components(
              queue = queue,
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
