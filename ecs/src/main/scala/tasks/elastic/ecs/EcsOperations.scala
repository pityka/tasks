package tasks.elastic.ecs

import cats.effect.IO
import cats.effect.Ref
import scala.jdk.CollectionConverters._
import software.amazon.awssdk.services.autoscaling.AutoScalingClient
import software.amazon.awssdk.services.ec2.Ec2Client
import software.amazon.awssdk.services.ecs.EcsClient
import software.amazon.awssdk.services.ecs.model._

sealed trait PlacementTarget

object PlacementTarget {
  case object External extends PlacementTarget
  final case class CapacityProvider(name: String) extends PlacementTarget {
    override def toString: String = s"capacity-provider:$name"
  }
}

final case class ContainerInstanceCapacity(
    arn: String,
    agentConnected: Boolean,
    remainingCpuUnits: Int,
    remainingMemoryMib: Int,
    remainingGpus: Int
)

final case class TaskPlacementFailure(reason: String, detail: Option[String]) {
  def render: String =
    detail.filter(_.nonEmpty).fold(reason)(d => s"$reason ($d)")

  def isCapacityShortage: Boolean =
    reason.startsWith("RESOURCE:") || reason == "AGENT"
}

final case class WorkerTaskSpec(
    taskDefinition: String,
    containerName: String,
    command: Option[List[String]],
    cpuUnits: Int,
    memoryMib: Int,
    gpus: Int,
    environment: Map[String, String]
)

trait EcsOperations {

  def runTask(
      spec: WorkerTaskSpec,
      target: PlacementTarget,
      clientToken: String
  ): IO[Either[List[TaskPlacementFailure], String]]

  def stopTask(taskArn: String, reason: String): IO[Unit]

  def placeableCapacity(
      target: PlacementTarget
  ): IO[List[ContainerInstanceCapacity]]

  def capacityProviderInfo(
      capacityProvider: String
  ): IO[CapacityProviderInfo]
}

object EcsOperations {

  val CpuUnitsPerVcpu: Int = 1024

  val externalInstanceFilter: String =
    "attribute:ecs.capability.external exists"

  def vcpuToCpuUnits(vcpu: Int): Int = vcpu * CpuUnitsPerVcpu

  def cpuUnitsToVcpu(units: Int): Int = units / CpuUnitsPerVcpu

  def fromClient(
      ecs: EcsClient,
      autoscaling: AutoScalingClient,
      ec2: Ec2Client,
      instanceTypeCache: Ref[IO, Map[String, InstanceTypeCapacity]],
      ecsConfig: EcsConfig
  ): EcsOperations =
    new FromSdkClient(ecs, autoscaling, ec2, instanceTypeCache, ecsConfig)

  private[ecs] def resourceInt(resources: List[Resource], name: String): Int =
    resources
      .find(r => Option(r.name).contains(name))
      .flatMap(r => Option(r.integerValue))
      .map(_.intValue)
      .getOrElse(0)

  private[ecs] def resourceGpuCount(resources: List[Resource]): Int =
    resources
      .find(r => Option(r.name).contains("GPU"))
      .map(r => r.stringSetValue.asScala.size)
      .getOrElse(0)

  private final class FromSdkClient(
      ecs: EcsClient,
      autoscaling: AutoScalingClient,
      ec2: Ec2Client,
      instanceTypeCache: Ref[IO, Map[String, InstanceTypeCapacity]],
      ecsConfig: EcsConfig
  ) extends EcsOperations {

    private val cluster = ecsConfig.cluster

    def capacityProviderInfo(
        capacityProvider: String
    ): IO[CapacityProviderInfo] =
      EcsCapacityDiscovery.describeCapacityProviderInfo(
        ecs = ecs,
        autoscaling = autoscaling,
        ec2 = ec2,
        cache = instanceTypeCache,
        capacityProvider = capacityProvider
      )

    private def strategy(
        capacityProvider: String
    ): List[CapacityProviderStrategyItem] = List(
      CapacityProviderStrategyItem.builder
        .capacityProvider(capacityProvider)
        .base(0)
        .weight(1)
        .build
    )

    private val sdkTags: List[Tag] =
      ecsConfig.tags.toList.map { case (k, v) =>
        Tag.builder.key(k).value(v).build
      }

    def runTask(
        spec: WorkerTaskSpec,
        target: PlacementTarget,
        clientToken: String
    ): IO[Either[List[TaskPlacementFailure], String]] = {
      val gpuRequirement =
        if (spec.gpus > 0)
          List(
            ResourceRequirement.builder
              .`type`(ResourceType.GPU)
              .value(spec.gpus.toString)
              .build
          )
        else Nil

      val environment = spec.environment.toList.map { case (k, v) =>
        KeyValuePair.builder.name(k).value(v).build
      }

      val containerOverrideBuilder = ContainerOverride.builder
        .name(spec.containerName)
        .cpu(spec.cpuUnits)
        .memory(spec.memoryMib)
        .resourceRequirements(gpuRequirement.asJava)
        .environment(environment.asJava)

      val containerOverride = spec.command match {
        case Some(cmd) => containerOverrideBuilder.command(cmd.asJava).build
        case None      => containerOverrideBuilder.build
      }

      val requestBuilderBase = RunTaskRequest.builder
        .cluster(cluster)
        .taskDefinition(spec.taskDefinition)
        .count(1)
        .startedBy(ecsConfig.startedBy)
        .clientToken(clientToken)
        .overrides(
          TaskOverride.builder.containerOverrides(containerOverride).build
        )

      // External (ECS Anywhere / on-prem) instances aren't members of any
      // capacity provider, so they're reachable only via launchType=EXTERNAL
      // + a placement constraint -- capacityProviderStrategy is the other,
      // mutually exclusive mechanism, used for the elastic pool.
      val requestBuilder = target match {
        case PlacementTarget.External =>
          requestBuilderBase
            .launchType(LaunchType.EXTERNAL)
            .placementConstraints(
              PlacementConstraint.builder
                .`type`(PlacementConstraintType.MEMBER_OF)
                .expression(EcsOperations.externalInstanceFilter)
                .build
            )
        case PlacementTarget.CapacityProvider(capacityProvider) =>
          requestBuilderBase.capacityProviderStrategy(
            strategy(capacityProvider).asJava
          )
      }

      val request =
        if (sdkTags.isEmpty) requestBuilder.build
        else requestBuilder.tags(sdkTags.asJava).build

      IO(
        scribe.info(
          s"ecs.runTask(cluster=$cluster, target=$target, " +
            s"taskDefinition=${spec.taskDefinition}, cpuUnits=${spec.cpuUnits}, " +
            s"memoryMiB=${spec.memoryMib}, gpus=${spec.gpus})"
        )
      ) *>
        IO.interruptible(ecs.runTask(request)).map { response =>
          response.tasks.asScala.toList.headOption.map(_.taskArn) match {
            case Some(arn) => Right(arn)
            case None =>
              Left(
                response.failures.asScala.toList.map { f =>
                  TaskPlacementFailure(
                    Option(f.reason).getOrElse("unknown"),
                    Option(f.detail)
                  )
                }
              )
          }
        }
    }

    def stopTask(taskArn: String, reason: String): IO[Unit] =
      IO(scribe.info(s"ecs.stopTask(cluster=$cluster, task=$taskArn)")) *>
        IO.interruptible {
          ecs.stopTask(
            StopTaskRequest.builder
              .cluster(cluster)
              .task(taskArn)
              .reason(reason)
              .build
          )
          ()
        }.handleErrorWith {
          case _: InvalidParameterException =>
            IO(
              scribe.warn(
                s"ecs.stopTask($taskArn) rejected as invalid; treating as already stopped"
              )
            )
          case e =>
            IO(scribe.error(s"ecs.stopTask($taskArn) failed", e)) *>
              IO.raiseError(e)
        }

    def placeableCapacity(
        target: PlacementTarget
    ): IO[List[ContainerInstanceCapacity]] = {
      def listArns(
          acc: List[String],
          token: Option[String]
      ): IO[List[String]] =
        IO.interruptible {
          val unfiltered = ListContainerInstancesRequest.builder
            .cluster(cluster)
            .status(ContainerInstanceStatus.ACTIVE)
          val filtered = target match {
            case PlacementTarget.External =>
              unfiltered.filter(EcsOperations.externalInstanceFilter)
            case PlacementTarget.CapacityProvider(_) =>
              unfiltered
          }
          val request =
            token.fold(filtered.build)(t => filtered.nextToken(t).build)
          val response = ecs.listContainerInstances(request)
          (
            response.containerInstanceArns.asScala.toList,
            Option(response.nextToken)
          )
        }.flatMap { case (arns, next) =>
          val updated = acc ::: arns
          next match {
            case Some(_) => listArns(updated, next)
            case None    => IO.pure(updated)
          }
        }

      listArns(Nil, None).flatMap { arns =>
        if (arns.isEmpty) IO.pure(Nil)
        else
          IO.interruptible {
            arns
              .grouped(100)
              .flatMap { chunk =>
                ecs
                  .describeContainerInstances(
                    DescribeContainerInstancesRequest.builder
                      .cluster(cluster)
                      .containerInstances(chunk.asJava)
                      .build
                  )
                  .containerInstances
                  .asScala
                  .toList
                  .filter { instance =>
                    target match {
                      case PlacementTarget.External => true
                      case PlacementTarget.CapacityProvider(capacityProvider) =>
                        Option(instance.capacityProviderName)
                          .contains(capacityProvider)
                    }
                  }
                  .map { instance =>
                    val remaining = instance.remainingResources.asScala.toList
                    ContainerInstanceCapacity(
                      arn = instance.containerInstanceArn,
                      agentConnected = instance.agentConnected,
                      remainingCpuUnits = resourceInt(remaining, "CPU"),
                      remainingMemoryMib = resourceInt(remaining, "MEMORY"),
                      remainingGpus = resourceGpuCount(remaining)
                    )
                  }
              }
              .toList
          }.map(_.filter(_.agentConnected))
      }
    }
  }
}
