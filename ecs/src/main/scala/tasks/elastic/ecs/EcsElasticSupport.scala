package tasks.elastic.ecs

import cats.effect.IO
import cats.effect.ExitCode
import cats.effect.kernel.Deferred
import org.ekrich.config.Config
import software.amazon.awssdk.services.ecs.EcsClient
import software.amazon.awssdk.regions.Region
import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros._

import tasks.deploy._
import tasks.elastic._
import tasks.shared._
import tasks.util._
import tasks.util.config._

class EcsHostConfig(val config: EcsConfig) extends HostConfigurationFromConfig

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

  val targetOrder: List[PlacementTarget] =
    List(PlacementTarget.External, PlacementTarget.CapacityProvider)

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
      case Left(error)           => IO.pure(Left(error))
      case Right(taskDefinition) => place(requestSize, taskDefinition)
    }

  private def place(
      requestSize: ResourceRequest,
      taskDefinition: String
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
      clientToken = java.util.UUID.randomUUID.toString
    )

    attempt(spec, EcsCreateNode.targetOrder, resources)
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
        ops
          .runTask(spec, target)
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
                  s"ECS placement via $target failed (${failures.map(_.render).mkString("; ")}); trying $rest"
                )
              ) *> attempt(spec, rest, resources)
            case Left(failures) => classify(target, failures, resources)
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
      requested: ResourceAvailable
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
      else IO(scribe.error(base)).as(base)

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
      config: Option[Config]
  ): cats.effect.Resource[IO, ElasticSupport] = {
    val ecsConfig = new EcsConfig(tasks.util.loadConfig(config))
    cats.effect.Resource.eval {
      IO {
        val region = EcsConfig.resolveRegion(ecsConfig.configuredRegion)
        val client = EcsClient.builder.region(Region.of(region)).build
        val ops = EcsOperations.fromClient(client, ecsConfig)
        val shutdown = new EcsShutdown(ops, ecsConfig.stopReason)

        scribe.info(s"ECS elastic backend: $ecsConfig")

        new ElasticSupport(
          hostConfig = Some(new EcsHostConfig(ecsConfig)),
          shutdownFromNodeRegistry = shutdown,
          shutdownFromWorker = shutdown,
          createNodeFactory = new EcsCreateNodeFactory(ops, ecsConfig, region),
          getNodeName = EcsGetNodeName,
          needsPackageServer = false
        )
      }
    }
  }
}
