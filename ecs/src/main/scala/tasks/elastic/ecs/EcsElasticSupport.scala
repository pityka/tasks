package tasks.elastic.ecs

import cats.effect.IO
import cats.effect.ExitCode
import cats.effect.Ref
import cats.effect.kernel.Deferred
import cats.syntax.parallel._
import software.amazon.awssdk.services.autoscaling.AutoScalingClient
import software.amazon.awssdk.services.ec2.Ec2Client
import software.amazon.awssdk.services.ecs.EcsClient
import software.amazon.awssdk.regions.Region
import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros._

import tasks.deploy._
import tasks.elastic._
import tasks.shared._
import tasks.util._
import tasks.util.config._

class EcsShutdown(ops: EcsOperations, stopReason: String)
    extends ShutdownNode
    with ShutdownSelfNode {

  def shutdownRunningNode(nodeName: RunningJobId): IO[Unit] =
    ops.stopTask(nodeName.value, stopReason)

  def shutdownRunningNode(
      exitCode: Deferred[IO, ExitCode],
      nodeName: RunningJobId
  ): IO[Unit] =
    shutdownRunningNode(nodeName)

  def shutdownPendingNode(nodeName: PendingJobId): IO[Unit] =
    ops.stopTask(nodeName.value, stopReason)
}

object EcsCreateNode {

  def targetOrder(capacityProviders: List[String]): List[PlacementTarget] =
    PlacementTarget.External :: capacityProviders.map(
      PlacementTarget.CapacityProvider(_)
    )

  def selectResources(
      requestSize: ResourceRequest,
      minCpu: Int,
      minMemory: Int
  ): ResourceAvailable = {
    val cpu = math.max(requestSize.cpu._2, minCpu)
    val memory = math.max(requestSize.memory, minMemory)
    val gpus = (0 until requestSize.gpu).toList
    ResourceAvailable(cpu, memory, requestSize.scratch, gpus, requestSize.image)
  }

  def renderCapacity(capacity: List[ContainerInstanceCapacity]): String =
    if (capacity.isEmpty)
      "no ACTIVE container instances"
    else
      capacity
        .map { c =>
          s"${c.arn.split('/').lastOption.getOrElse(c.arn)}" +
            s"[vcpu=${EcsOperations.cpuUnitsToVcpu(c.remainingCpuUnits)}" +
            s",memMiB=${c.remainingMemoryMib},gpu=${c.remainingGpus}]"
        }
        .mkString(", ")

  private[ecs] def canHostRequest(
      info: CapacityProviderInfo,
      request: ResourceAvailable
  ): Boolean =
    info.instanceTypes.isEmpty ||
      info.instanceTypes.exists(instanceType =>
        instanceType.vcpus >= request.cpu &&
          instanceType.memoryMib >= request.memory &&
          instanceType.gpus >= request.gpu.size
      )

  private[ecs] def hasRoomFor(
      capacity: List[ContainerInstanceCapacity],
      request: ResourceAvailable
  ): Boolean =
    capacity.exists(instance =>
      EcsOperations.cpuUnitsToVcpu(instance.remainingCpuUnits) >= request.cpu &&
        instance.remainingMemoryMib >= request.memory &&
        instance.remainingGpus >= request.gpu.size
    )
}

class EcsCreateNode(
    masterAddress: SimpleSocketAddress,
    masterPrefix: String,
    codeAddress: CodeAddress,
    ops: EcsOperations,
    ecsConfig: EcsConfig,
    resolvedRegion: String
) extends CreateNode {

  def requestOneNewJobFromJobScheduler(
      requestSize: ResourceRequest
  )(implicit
      config: TasksConfig
  ): IO[Either[String, (PendingJobId, ResourceAvailable)]] =
    ecsConfig.resolveTaskDefinition(requestSize.image) match {
      case Left(error) => IO.pure(Left(error))
      case Right(taskDefinition) =>
        EcsAttributes.placementExpression(requestSize.nodeSelector) match {
          case Left(error) => IO.pure(Left(error))
          case Right(placementExpression) =>
            place(requestSize, taskDefinition, placementExpression)
        }
    }

  private def place(
      requestSize: ResourceRequest,
      taskDefinition: String,
      placementExpression: Option[String]
  )(implicit
      config: TasksConfig
  ): IO[Either[String, (PendingJobId, ResourceAvailable)]] = {
    val resources = EcsCreateNode.selectResources(
      requestSize,
      ecsConfig.minimumCpu,
      ecsConfig.minimumMemory
    )

    val environment = Map(
      "AWS_REGION" -> resolvedRegion,
      "AWS_DEFAULT_REGION" -> resolvedRegion
    ) ++ Deployment.workerEnvironment(
      memory = resources.memory,
      cpu = resources.cpu,
      scratch = resources.scratch,
      gpus = resources.gpu,
      masterAddress = masterAddress,
      masterPrefix = masterPrefix,
      followerHostname = None,
      followerExternalHostname = None,
      followerMayUseArbitraryPort = true,
      followerNodeName = None,
      image = requestSize.image,
      workerHealthUrlFile = config.workerHealthUrlFile.map(_.getAbsolutePath),
      labels = resources.labels
    )(config) ++ ecsConfig.extraEnvironment

    val spec = WorkerTaskSpec(
      taskDefinition = taskDefinition,
      containerName = ecsConfig.containerName,
      command = None,
      cpuUnits = EcsOperations.vcpuToCpuUnits(resources.cpu),
      memoryMib = resources.memory,
      gpus = resources.gpu.size,
      environment = environment,
      placementExpression = placementExpression
    )

    screen(
      EcsCreateNode.targetOrder(ecsConfig.capacityProviders),
      resources
    ).attempt.flatMap {
      case Left(e) =>
        val msg =
          "ECS could not discover the capacity of " +
            s"[${ecsConfig.capacityProviders.mkString(",")}] on cluster " +
            s"${ecsConfig.cluster}, so no worker task was placed: " +
            s"${e.getClass.getSimpleName}: " +
            Option(e.getMessage).getOrElse("<no message>") +
            ". Discovery needs ecs:DescribeCapacityProviders, " +
            "autoscaling:DescribeAutoScalingGroups, ec2:DescribeInstanceTypes " +
            "and ec2:DescribeLaunchTemplateVersions. This request will be retried."
        IO(scribe.error(msg, e)).as(
          Left(msg): Either[String, (PendingJobId, ResourceAvailable)]
        )
      case Right((attemptable, skipped)) =>
        attempt(spec, attemptable, resources).map {
          case Left(msg) if skipped.nonEmpty =>
            Left(s"$msg. Skipped: ${skipped.mkString("; ")}")
          case other => other
        }
    }
  }

  private def screen(
      targets: List[PlacementTarget],
      request: ResourceAvailable
  ): IO[(List[PlacementTarget], List[String])] =
    targets
      .parTraverse {
        case PlacementTarget.External =>
          IO.pure(
            (
              Some(PlacementTarget.External: PlacementTarget),
              Option.empty[String]
            )
          )
        case target @ PlacementTarget.CapacityProvider(_) =>
          skipReason(target, request).map {
            case Some(reason) => (None, Some(reason))
            case None => (Some(target: PlacementTarget), Option.empty[String])
          }
      }
      .flatTap { screened =>
        val skipped = screened.flatMap(_._2)
        if (skipped.isEmpty) IO.unit
        else
          IO(
            scribe.info(
              s"ECS skipping ${skipped.size} capacity provider(s) for request " +
                s"vcpu=${request.cpu} memMiB=${request.memory} " +
                s"gpu=${request.gpu.size}: ${skipped.mkString("; ")}"
            )
          )
      }
      .map(screened => (screened.flatMap(_._1), screened.flatMap(_._2)))

  private def skipReason(
      target: PlacementTarget.CapacityProvider,
      request: ResourceAvailable
  ): IO[Option[String]] =
    ops
      .capacityProviderInfo(target.name)
      .flatMap[Option[String]] { info =>
        if (!EcsCreateNode.canHostRequest(info, request))
          IO.pure(
            Some(
              s"${target.name} scales instance types " +
                s"${EcsCapacityDiscovery.renderInstanceTypes(info.instanceTypes)} " +
                s"and none of them can host vcpu=${request.cpu} " +
                s"memMiB=${request.memory} gpu=${request.gpu.size}"
            )
          )
        else if (info.canScaleOut) IO.pure(None)
        else
          ops.placeableCapacity(target).map { capacity =>
            if (EcsCreateNode.hasRoomFor(capacity, request)) None
            else
              Some(
                s"${target.name} is at its auto scaling group maximum and " +
                  s"none of its container instances has room " +
                  s"(${EcsCreateNode.renderCapacity(capacity)})"
              )
          }
      }

  // External (on-prem/ECS-Anywhere, a fixed pool that doesn't autoscale) is
  // tried first; RunTask against it fails fast when nothing there fits, no
  // upfront capacity check needed. Falls through to the capacity provider
  // (elastic, can always scale out) on any failure. Mirrors
  // BatchCreateNode's queue fallback, just via try-then-fall-through instead
  // of a pre-check, since External's failure mode is cheap here.
  private def attempt(
      spec: WorkerTaskSpec,
      targets: List[PlacementTarget],
      resources: ResourceAvailable
  ): IO[Either[String, (PendingJobId, ResourceAvailable)]] =
    targets match {
      case Nil =>
        IO.pure(
          Left("ECS: no placement target configured"): Either[
            String,
            (PendingJobId, ResourceAvailable)
          ]
        )
      case target :: rest =>
        IO(java.util.UUID.randomUUID.toString)
          .flatMap(clientToken => ops.runTask(spec, target, clientToken))
          .flatMap {
            case Right(taskArn) =>
              IO(scribe.info(s"ECS worker task started: $taskArn")).as(
                Right((PendingJobId(taskArn), resources)): Either[
                  String,
                  (PendingJobId, ResourceAvailable)
                ]
              )
            case Left(failures) if rest.nonEmpty =>
              IO(
                scribe.info(
                  s"ECS placement via $target failed (${failures.map(_.render).mkString("; ")}); trying ${rest.mkString(", ")}"
                )
              ) *> attempt(spec, rest, resources)
            case Left(failures) =>
              classify(target, failures, resources, spec.placementExpression)
          }
          .handleErrorWith { e =>
            val msg =
              s"${e.getClass.getSimpleName}: " +
                Option(e.getMessage).getOrElse("<no message>")
            IO(scribe.error(s"ecs.runTask failed: $msg", e)).as(
              Left(msg): Either[String, (PendingJobId, ResourceAvailable)]
            )
          }
    }

  private def classify(
      target: PlacementTarget,
      failures: List[TaskPlacementFailure],
      requested: ResourceAvailable,
      placementExpression: Option[String]
  ): IO[Either[String, (PendingJobId, ResourceAvailable)]] = {
    val rendered =
      if (failures.isEmpty) "RunTask returned neither a task nor a failure"
      else failures.map(_.render).mkString("; ")
    val base =
      s"ECS could not place a worker task via $target on cluster " +
        s"${ecsConfig.cluster} for request vcpu=${requested.cpu} " +
        s"memMiB=${requested.memory} gpu=${requested.gpu.size}: $rendered"

    val capacityShortage =
      failures.nonEmpty && failures.forall(_.isCapacityShortage)

    val attributeMismatch =
      failures.nonEmpty && failures.forall(_.isAttributeMismatch)

    val logged =
      if (capacityShortage)
        ops
          .placeableCapacity(target)
          .map(capacity =>
            s"$base. Remaining capacity: ${EcsCreateNode.renderCapacity(capacity)}"
          )
          .handleErrorWith(_ => IO.pure(base))
          .flatTap(msg =>
            IO(
              scribe.warn(
                s"$msg. This request will be retried."
              )
            )
          )
      else if (attributeMismatch) {
        val explained =
          s"$base. No ACTIVE container instance satisfies the placement " +
            s"constraint ${placementExpression.getOrElse("<none>")} derived " +
            "from the node selector of this request. A placement constraint " +
            "filters the instances which are already registered, it does not " +
            "make the auto scaling group launch a matching one, so retrying " +
            "this request cannot help. Set the attribute on the container " +
            "instances (ECS_INSTANCE_ATTRIBUTES in /etc/ecs/ecs.config, or " +
            "the PutAttributes API), or drop the node selector."
        IO(scribe.error(explained)).as(explained)
      } else IO(scribe.error(base)).as(base)

    logged.map(msg => Left(msg))
  }

  override def convertRunningToPending(
      p: RunningJobId
  ): IO[Option[PendingJobId]] =
    IO.pure(Some(PendingJobId(p.value)))
}

class EcsCreateNodeFactory(
    ops: EcsOperations,
    ecsConfig: EcsConfig,
    resolvedRegion: String
) extends CreateNodeFactory {
  def apply(
      master: SimpleSocketAddress,
      masterPrefix: String,
      codeAddress: CodeAddress
  ) =
    new EcsCreateNode(
      masterAddress = master,
      masterPrefix = masterPrefix,
      codeAddress = codeAddress,
      ops = ops,
      ecsConfig = ecsConfig,
      resolvedRegion = resolvedRegion
    )
}

private[ecs] final case class EcsTaskMetadata(TaskARN: String)

private[ecs] object EcsTaskMetadata {
  implicit val codec: JsonValueCodec[EcsTaskMetadata] = JsonCodecMaker.make
}

object EcsGetNodeName extends GetNodeName {

  private val ecsMetadataUriVariable = "ECS_CONTAINER_METADATA_URI_V4"

  private def fetchTaskArn(metadataUri: String): IO[String] =
    IO.interruptible {
      val client = java.net.http.HttpClient.newHttpClient()
      val request = java.net.http.HttpRequest
        .newBuilder(java.net.URI.create(metadataUri + "/task"))
        .GET()
        .build()
      val response = client.send(
        request,
        java.net.http.HttpResponse.BodyHandlers.ofString()
      )
      readFromString[EcsTaskMetadata](response.body()).TaskARN
    }

  private def hostname: IO[RunningJobId] =
    IO(RunningJobId(java.net.InetAddress.getLocalHost.getHostName))

  def getNodeName(config: TasksConfig): IO[RunningJobId] = {
    val explicit = config.nodeName
    if (explicit.nonEmpty) IO.pure(RunningJobId(explicit))
    else
      Option(System.getenv(ecsMetadataUriVariable)) match {
        case Some(uri) =>
          fetchTaskArn(uri).map(RunningJobId(_)).handleErrorWith { e =>
            IO(
              scribe.error(
                s"Failed to read TaskARN from $ecsMetadataUriVariable; " +
                  "falling back to hostname. Shutdown of this node from " +
                  "the registry will not work.",
                e
              )
            ) *> hostname
          }
        case None =>
          IO(
            scribe.warn(
              s"$ecsMetadataUriVariable is not set; falling back to hostname " +
                "as node name. Shutdown of this node from the registry will not work."
            )
          ) *> hostname
      }
  }
}

object EcsElasticSupport {

  def apply(
      ecsConfig: EcsConfig
  ): cats.effect.Resource[IO, ElasticSupport] = {
    cats.effect.Resource.eval {
      for {
        instanceTypeCache <- Ref.of[IO, Map[String, InstanceTypeCapacity]](
          Map.empty
        )
        support <- IO {
          val region = EcsConfig.resolveRegion(ecsConfig.region)
          val client = EcsClient.builder.region(Region.of(region)).build
          val autoscaling =
            AutoScalingClient.builder.region(Region.of(region)).build
          val ec2 = Ec2Client.builder.region(Region.of(region)).build
          val ops = EcsOperations.fromClient(
            client,
            autoscaling,
            ec2,
            instanceTypeCache,
            ecsConfig
          )
          val shutdown = new EcsShutdown(ops, ecsConfig.stopReason)

          scribe.info(s"ECS elastic backend: $ecsConfig")

          new ElasticSupport(
            hostConfig = Some((tasksConfig: TasksConfig) =>
              new DefaultHostConfigurationFromConfig()(tasksConfig)
            ),
            shutdownFromNodeRegistry = shutdown,
            shutdownFromWorker = shutdown,
            createNodeFactory =
              new EcsCreateNodeFactory(ops, ecsConfig, region),
            getNodeName = EcsGetNodeName,
            needsPackageServer = false
          )
        }
      } yield support
    }
  }
}
