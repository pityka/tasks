/*
 * The MIT License
 *
 * Copyright (c) 2016 Istvan Bartha
 */
package tasks.elastic.ec2

import cats.effect._
import cats.effect.kernel.Deferred
import cats.effect.kernel.Ref
import cats.syntax.all._
import com.amazonaws.ec2._
import org.ekrich.config.Config
import tasks.deploy.HostConfigurationFromConfig
import tasks.elastic._
import tasks.shared._
import tasks.util._
import tasks.util.config._
import tasks.util.message.Node

/** EC2 Elastic Support — smithy4s-based backend using AWS spot instances.
  *
  * Master-side behaviour:
  *   - Discovers its own sizing via `DescribeInstances` /
  *     `DescribeInstanceTypes` — no hardware sizes declared in config.
  *   - When creating a worker, picks the first configured candidate instance
  *     type that fits the request and submits a spot request.
  *   - Cancelling a pending spot request also terminates the instance if the
  *     spot request has been fulfilled.
  *
  * Worker-side behaviour:
  *   - The user-data script (see [[EC2UserData]]) mounts instance-store NVMe to
  *     `tasks.elastic.aws.instanceStorageMountPoint` and points the JVM's
  *     `java.io.tmpdir` there. `availableScratch` in the host config is then
  *     derived from `File.getUsableSpace` on that mount.
  *   - Self-shutdown completes the exit `Deferred` — the wrapping user-data
  *     script terminates the EC2 instance once the JVM exits.
  */
object EC2ElasticSupport {

  def apply(configOverride: Option[Config]): Resource[IO, ElasticSupport] = {
    val ec2Config = new EC2Config(loadConfig(configOverride))
    for {
      _ <- Resource.eval(
        IO(scribe.info(s"EC2 elastic backend initialising: $ec2Config"))
      )
      ec2Client <- EC2ClientBuilder.make(ec2Config.awsRegion)
      ops = EC2Operations.fromClient(ec2Client)
      registeredInstances <- Resource.eval(Ref.of[IO, Set[String]](Set.empty))
      // Discover candidate instance-type sizing once at startup.
      typeInfoList <- Resource.eval(
        ops.describeInstanceTypes(ec2Config.candidateInstanceTypes)
      )
      _ <- Resource.eval(
        IO(
          scribe.info(
            s"EC2 discovered ${typeInfoList.size}/${ec2Config.candidateInstanceTypes.size} candidate instance types"
          )
        )
      )
      typeMap = typeInfoList
        .map(t => t.instanceType.map(_.stringValue).getOrElse("") -> t)
        .toMap
      hostConfig = new EC2MasterFollower(ec2Config, ops)
      support = assemble(
        ec2Config,
        ops,
        typeMap,
        registeredInstances,
        Some(hostConfig)
      )
      _ <- Resource.make(IO.pure(support)) { _ =>
        if (ec2Config.terminateMaster)
          IO(scribe.info("EC2 release: terminating master")) *>
            EC2Metadata.instanceId
              .flatMap(ops.terminateInstance)
              .handleErrorWith { e =>
                IO(
                  scribe.error(
                    "Failed to terminate master instance on release",
                    e
                  )
                )
              }
        else IO.unit
      }
    } yield support
  }

  private[ec2] def assemble(
      ec2Config: EC2Config,
      ops: EC2Operations,
      typeMap: Map[String, InstanceTypeInfo],
      registeredInstances: Ref[IO, Set[String]],
      hostConfig: Option[tasks.deploy.HostConfiguration]
  ): ElasticSupport = {
    val converter = new EC2ConvertRunningToPending(ops)
    new ElasticSupport(
      hostConfig = hostConfig,
      shutdownFromNodeRegistry = new EC2Shutdown(ops, registeredInstances),
      shutdownFromWorker = new EC2SelfShutdown,
      createNodeFactory = new EC2CreateNodeFactory(
        ec2Config,
        ops,
        typeMap,
        converter,
        registeredInstances
      ),
      getNodeName = EC2GetNodeName,
      convertRunningToPending = converter
    )
  }
}

class EC2ConvertRunningToPending(ops: EC2Operations)
    extends ConvertRunningToPending {

  override def convertRunningToPending(
      p: RunningJobId
  ): IO[Option[PendingJobId]] =
    ops
      .describeSpotRequestByInstance(p.value)
      .map(_.flatMap(_.spotInstanceRequestId))
      .flatMap {
        case Some(spotRequestId) =>
          IO.pure(Some(PendingJobId(spotRequestId)))
        case None =>
          IO(
            scribe.warn(
              "NoSpotRequestForInstance",
              p,
              scribe.data(
                "explain",
                "No spot request is associated with this instance. Using the instance id as pending id."
              )
            )
          ).as(Some(PendingJobId(p.value)))
      }
      .handleErrorWith { e =>
        IO(
          scribe.error(
            "SpotRequestLookupFailed",
            p,
            scribe.data(
              "explain",
              "Using the instance id as pending id."
            ),
            e
          )
        ).as(Some(PendingJobId(p.value)))
      }
}

/** Self-shutdown from within the worker JVM: signal the JVM to exit cleanly.
  * The wrapping user-data script terminates the instance once the process is
  * gone (see [[EC2UserData]]).
  */
class EC2SelfShutdown extends ShutdownSelfNode {
  def shutdownRunningNode(
      exitCode: Deferred[IO, ExitCode],
      nodeName: RunningJobId
  ): IO[Unit] =
    IO(scribe.info(s"EC2 worker self-shutdown: $nodeName")) *>
      exitCode.complete(ExitCode.Success).void
}

/** Master-initiated shutdown. `shutdownRunningNode` terminates the running
  * instance; `shutdownPendingNode` cancels the spot request AND terminates the
  * instance if the request was already fulfilled.
  */
class EC2Shutdown(
    ops: EC2Operations,
    registeredInstances: Ref[IO, Set[String]]
) extends ShutdownNode {

  def shutdownRunningNode(nodeName: RunningJobId): IO[Unit] =
    IO(scribe.info(s"EC2 master shutdown of running $nodeName")) *>
      registeredInstances.update(_ - nodeName.value) *>
      ops.terminateInstance(nodeName.value)

  def shutdownPendingNode(nodeName: PendingJobId): IO[Unit] =
    registeredInstances.get.flatMap { registered =>
      if (registered.contains(nodeName.value))
        IO(
          scribe.warn(
            "SkipShutdownOfRegisteredWorker",
            nodeName,
            scribe.data(
              "explain",
              "This pending id is the instance id of a worker which already registered. Not terminating it."
            )
          )
        )
      else
        IO(scribe.info(s"EC2 master cancel of pending $nodeName")) *>
          ops.cancelSpotRequest(nodeName.value) *>
          terminateInstanceLeftBehindBy(nodeName.value)
    }

  private def terminateInstanceLeftBehindBy(spotRequestId: String): IO[Unit] =
    ops.describeSpotRequestById(spotRequestId).flatMap { request =>
      request.flatMap(_.instanceId).map(InstanceId.value) match {
        case None => IO.unit
        case Some(instanceId) =>
          registeredInstances.get.flatMap { registered =>
            if (registered.contains(instanceId))
              IO(
                scribe.warn(
                  "SkipShutdownOfRegisteredWorker",
                  scribe.data(
                    Map(
                      "instance-id" -> instanceId,
                      "spot-request-id" -> spotRequestId,
                      "explain" -> "The cancelled spot request spawned an instance which already registered as a worker. Not terminating it."
                    )
                  )
                )
              )
            else
              IO(
                scribe.info(
                  s"spot $spotRequestId already spawned $instanceId — terminating"
                )
              ) *> ops.terminateInstance(instanceId)
          }
      }
    }
}

object EC2GetNodeName extends GetNodeName {
  def getNodeName(config: TasksConfig): IO[RunningJobId] =
    EC2Metadata.instanceId.map(RunningJobId.apply)
}

class EC2CreateNodeFactory(
    ec2Config: EC2Config,
    ops: EC2Operations,
    typeMap: Map[String, InstanceTypeInfo],
    converter: ConvertRunningToPending,
    registeredInstances: Ref[IO, Set[String]]
) extends CreateNodeFactory {
  def apply(
      master: SimpleSocketAddress,
      masterPrefix: String,
      codeAddress: CodeAddress
  ): CreateNode =
    new EC2CreateNode(
      master,
      masterPrefix,
      codeAddress,
      ec2Config,
      ops,
      typeMap,
      converter,
      registeredInstances
    )
}

class EC2CreateNode(
    masterAddress: SimpleSocketAddress,
    masterPrefix: String,
    codeAddress: CodeAddress,
    ec2Config: EC2Config,
    ops: EC2Operations,
    typeMap: Map[String, InstanceTypeInfo],
    converter: ConvertRunningToPending,
    registeredInstances: Ref[IO, Set[String]]
) extends CreateNode {

  override def convertRunningToPending(
      p: RunningJobId
  ): IO[Option[PendingJobId]] =
    converter.convertRunningToPending(p)

  override def initializeNode(node: Node): IO[Unit] =
    registeredInstances.update(_ + node.name.value) *>
      ops.createTags(List(node.name.value), ec2Config.instanceTags)

  def requestOneNewJobFromJobScheduler(
      requestSize: ResourceRequest
  )(implicit
      config: TasksConfig
  ): IO[Either[String, (PendingJobId, ResourceAvailable)]] = {
    if (ec2Config.spotPrice > ec2Config.spotPriceCap)
      IO(
        scribe.error(
          s"spotPrice ${ec2Config.spotPrice} exceeds cap ${ec2Config.spotPriceCap}"
        )
      ) *>
        IO.pure(
          Left(
            s"spotPrice ${ec2Config.spotPrice} exceeds cap ${ec2Config.spotPriceCap}"
          )
        )
    else
      pickInstanceType(requestSize) match {
        case None =>
          IO(
            scribe.warn(s"No candidate instance type satisfies $requestSize")
          ) *>
            IO.pure(
              Left(s"No candidate instance type satisfies $requestSize")
            )
        case Some((typeName, info)) =>
          val available =
            EC2CreateNode.resourceAvailable(info, requestSize.image)
          val userData = EC2UserData.script(
            memory = available.memory,
            cpu = available.cpu,
            scratch = available.scratch,
            gpus = available.gpu,
            masterAddress = masterAddress,
            masterPrefix = masterPrefix,
            codeDownload = Uri(
              scheme = "http",
              hostname = codeAddress.address.getHostName,
              port = codeAddress.address.getPort,
              path = "/"
            ),
            image = requestSize.image,
            labels = requestSize.nodeSelector
              .fold(Set.empty[String])(EC2CreateNode.labelsFromSelector),
            mountPoint = ec2Config.instanceStorageMountPoint
          )
          val launch = buildLaunchSpec(typeName, userData)
          submit(launch, available)
      }
  }

  private def pickInstanceType(
      req: ResourceRequest
  ): Option[(String, InstanceTypeInfo)] =
    ec2Config.candidateInstanceTypes.iterator
      .flatMap(n => typeMap.get(n).map(n -> _))
      .find { case (_, info) => EC2CreateNode.fits(info, req) }

  private def buildLaunchSpec(
      instanceType: String,
      userData: String
  ): RequestSpotLaunchSpecification = {
    val securityGroups =
      if (ec2Config.securityGroups.isEmpty) None
      else Some(ec2Config.securityGroups.map(SecurityGroupId.apply))
    val iam = ec2Config.iamRoleArn.map { arn =>
      IamInstanceProfileSpecification(arn = Some(arn))
    }
    val placement = ec2Config.placementGroup.map { pg =>
      SpotPlacement(groupName = Some(PlacementGroupName(pg)))
    }
    RequestSpotLaunchSpecification(
      securityGroupIds = securityGroups,
      iamInstanceProfile = iam,
      imageId = Some(ImageId(ec2Config.amiID)),
      instanceType = Some(InstanceType.fromStringOrUnknown(instanceType)),
      keyName = ec2Config.keyName.map(KeyPairNameWithResolver.apply),
      placement = placement,
      subnetId = Some(SubnetId(ec2Config.subnetId)),
      userData = Some(SensitiveUserData(EC2CreateNode.gzipBase64(userData)))
    )
  }

  private def submit(
      launch: RequestSpotLaunchSpecification,
      available: ResourceAvailable
  ): IO[Either[String, (PendingJobId, ResourceAvailable)]] = {
    val price =
      if (ec2Config.spotPrice > 0) Some(f"${ec2Config.spotPrice}%.4f")
      else None
    ops
      .requestSpotInstance(launch, price, ec2Config.instanceTags)
      .map { sri =>
        Right(PendingJobId(sri) -> available): Either[
          String,
          (PendingJobId, ResourceAvailable)
        ]
      }
      .handleErrorWith { e =>
        val msg =
          s"${e.getClass.getSimpleName}: " +
            Option(e.getMessage).getOrElse("<no message>")
        IO(scribe.error(s"requestSpotInstance failed: $msg", e)) *>
          IO.pure(
            Left(msg): Either[String, (PendingJobId, ResourceAvailable)]
          )
      }
  }
}

object EC2CreateNode {

  /** True iff `info` has enough CPU / memory / GPUs to satisfy `req`. */
  def fits(info: InstanceTypeInfo, req: ResourceRequest): Boolean = {
    val vcpu = info.vCpuInfo.flatMap(_.defaultVCpus).map(_.value).getOrElse(0)
    val memMib =
      info.memoryInfo.flatMap(_.sizeInMiB).map(_.value).getOrElse(0L)
    val gpuCount = info.gpuInfo
      .flatMap(_.gpus)
      .getOrElse(Nil)
      .flatMap(_.count.map(_.value))
      .sum
    vcpu >= req.cpu._1 && memMib >= req.memory.toLong &&
    gpuCount >= req.gpu && scratchMiB(info) >= req.scratch
  }

  private[ec2] def scratchMiB(info: InstanceTypeInfo): Int = {
    val gb = info.instanceStorageInfo
      .flatMap(_.totalSizeInGB)
      .map(_.value)
      .getOrElse(0L)
    (gb.toDouble * 1000d * 1000d * 1000d / (1024d * 1024d)).toInt
  }

  /** Derive a `ResourceAvailable` reflecting the actual instance-type sizing.
    * Scratch is the sum of instance-store disks (MiB); if the instance has no
    * instance store, scratch reports 0 — the master should choose an
    * instance-type that includes storage if it wants scratch.
    */
  def resourceAvailable(
      info: InstanceTypeInfo,
      image: Option[String]
  ): ResourceAvailable = {
    val vcpu = info.vCpuInfo.flatMap(_.defaultVCpus).map(_.value).getOrElse(0)
    val memMib =
      info.memoryInfo.flatMap(_.sizeInMiB).map(_.value.toInt).getOrElse(0)
    val gpuCount = info.gpuInfo
      .flatMap(_.gpus)
      .getOrElse(Nil)
      .flatMap(_.count.map(_.value))
      .sum
    ResourceAvailable(
      cpu = vcpu,
      memory = memMib,
      scratch = scratchMiB(info),
      gpu = (0 until gpuCount).toList,
      image = image,
      labels = Set.empty
    )
  }

  def labelsFromSelector(sel: NodeSelector): Set[String] = sel match {
    case NodeSelector.Always     => Set.empty
    case NodeSelector.Has(label) => Set(label)
    case NodeSelector.Not(_)     => Set.empty
    case NodeSelector.And(xs) => xs.iterator.flatMap(labelsFromSelector).toSet
    case NodeSelector.Or(xs)  => xs.iterator.flatMap(labelsFromSelector).toSet
  }

  def gzipBase64(str: String): String = {
    val out = new java.io.ByteArrayOutputStream()
    val gzip = new java.util.zip.GZIPOutputStream(out)
    try {
      gzip.write(
        str.getBytes(java.nio.charset.StandardCharsets.UTF_8)
      )
    } finally gzip.close()
    java.util.Base64.getEncoder.encodeToString(out.toByteArray)
  }
}

/** Host configuration for the master node. Discovers its own CPU / memory /
  * scratch via `DescribeInstanceTypes` (no config-declared sizing).
  *
  * The IMDS + DescribeInstanceTypes calls happen once at construction inside
  * `lazy val`s — same pattern as the previous implementation.
  */
class EC2MasterFollower(val config: EC2Config, ops: EC2Operations)
    extends HostConfigurationFromConfig {

  private def runSync[A](io: IO[A]): A =
    io.unsafeRunSync()(cats.effect.unsafe.implicits.global)

  private lazy val myhostname: String = runSync(
    IO(scribe.info("EC2MasterFollower: fetching local-hostname from IMDS")) *>
      EC2Metadata.localHostname
  )

  private lazy val instanceType: String = runSync(
    IO(scribe.info("EC2MasterFollower: fetching instance-type from IMDS")) *>
      EC2Metadata.instanceType
  )

  private lazy val info: InstanceTypeInfo = runSync(
    for {
      _ <- IO(
        scribe.info(
          s"EC2MasterFollower: describing instance-type $instanceType"
        )
      )
      list <- ops.describeInstanceTypes(List(instanceType))
      first <- list.headOption match {
        case Some(v) => IO.pure(v)
        case None =>
          IO.raiseError(
            new RuntimeException(
              s"DescribeInstanceTypes returned no info for $instanceType"
            )
          )
      }
    } yield first
  )

  override lazy val myAddress: SimpleSocketAddress =
    SimpleSocketAddress(myhostname, myPort)

  override lazy val availableCPU: Int =
    info.vCpuInfo.flatMap(_.defaultVCpus).map(_.value).getOrElse(1)

  override lazy val availableMemory: Int =
    info.memoryInfo.flatMap(_.sizeInMiB).map(_.value.toInt).getOrElse(1024)

  /** Scratch space reported to the queue is the size of the volume backing the
    * JVM's temp dir. On workers this is `/instancestorage` (set by the
    * user-data script). On the master it defaults to whatever the OS's temp dir
    * is on.
    */
  override lazy val availableScratch: Int = {
    val dir = new java.io.File(sys.props.getOrElse("java.io.tmpdir", "/tmp"))
    val bytes = dir.getUsableSpace
    if (bytes <= 0) 0 else (bytes / (1024L * 1024L)).toInt
  }
}
