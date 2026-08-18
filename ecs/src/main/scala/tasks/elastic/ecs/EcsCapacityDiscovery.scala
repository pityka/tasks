package tasks.elastic.ecs

import cats.effect.IO
import cats.effect.Ref
import scala.jdk.CollectionConverters._
import software.amazon.awssdk.services.autoscaling.AutoScalingClient
import software.amazon.awssdk.services.autoscaling.model.DescribeAutoScalingGroupsRequest
import software.amazon.awssdk.services.ec2.Ec2Client
import software.amazon.awssdk.services.ec2.model.DescribeInstanceTypesRequest
import software.amazon.awssdk.services.ec2.model.DescribeLaunchTemplateVersionsRequest
import software.amazon.awssdk.services.ecs.EcsClient
import software.amazon.awssdk.services.ecs.model.DescribeCapacityProvidersRequest

final case class InstanceTypeCapacity(vcpus: Int, memoryMib: Int, gpus: Int)

final case class CapacityProviderInfo(
    name: String,
    instanceTypes: List[InstanceTypeCapacity],
    canScaleOut: Boolean
)

object EcsCapacityDiscovery {

  private val defaultLaunchTemplateVersion: String = "$Default"

  private def unknownFit(capacityProvider: String): CapacityProviderInfo =
    CapacityProviderInfo(capacityProvider, Nil, canScaleOut = true)

  private[ecs] def autoScalingGroupName(arn: String): Option[String] =
    Option(arn)
      .filter(_.contains("/"))
      .map(a => a.substring(a.lastIndexOf('/') + 1))
      .filter(_.nonEmpty)

  private[ecs] final case class LaunchTemplateRef(
      id: Option[String],
      name: Option[String],
      version: Option[String]
  )

  private[ecs] final case class AutoScalingGroupDetails(
      instanceTypes: List[String],
      launchTemplate: Option[LaunchTemplateRef],
      instanceCount: Int,
      desiredCapacity: Int,
      maxSize: Int
  ) {
    def canScaleOut: Boolean =
      instanceCount < desiredCapacity || desiredCapacity < maxSize
  }

  private final case class DescribedCapacityProvider(
      name: String,
      autoScalingGroupArn: Option[String]
  )

  private def describeCapacityProvider(
      ecs: EcsClient,
      capacityProvider: String
  ): IO[Option[DescribedCapacityProvider]] =
    IO.interruptible {
      ecs
        .describeCapacityProviders(
          DescribeCapacityProvidersRequest.builder
            .capacityProviders(capacityProvider)
            .build
        )
        .capacityProviders
        .asScala
        .toList
        .headOption
        .map(provider =>
          DescribedCapacityProvider(
            name = capacityProvider,
            autoScalingGroupArn = Option(provider.autoScalingGroupProvider)
              .flatMap(asg => Option(asg.autoScalingGroupArn))
              .filter(_.nonEmpty)
          )
        )
    }

  private def describeAutoScalingGroup(
      autoscaling: AutoScalingClient,
      groupName: String
  ): IO[Option[AutoScalingGroupDetails]] =
    IO.interruptible {
      autoscaling
        .describeAutoScalingGroups(
          DescribeAutoScalingGroupsRequest.builder
            .autoScalingGroupNames(groupName)
            .build
        )
        .autoScalingGroups
        .asScala
        .toList
        .headOption
        .map { group =>
          val mixed = Option(group.mixedInstancesPolicy)
            .flatMap(policy => Option(policy.launchTemplate))

          val overrideTypes = mixed
            .map(
              _.overrides.asScala.toList
                .flatMap(o => Option(o.instanceType))
            )
            .getOrElse(Nil)
            .filter(_.nonEmpty)

          val runningTypes = group.instances.asScala.toList
            .flatMap(instance => Option(instance.instanceType))
            .filter(_.nonEmpty)

          val launchTemplate = mixed
            .flatMap(template => Option(template.launchTemplateSpecification))
            .orElse(Option(group.launchTemplate))
            .map(spec =>
              LaunchTemplateRef(
                id = Option(spec.launchTemplateId).filter(_.nonEmpty),
                name = Option(spec.launchTemplateName).filter(_.nonEmpty),
                version = Option(spec.version).filter(_.nonEmpty)
              )
            )

          AutoScalingGroupDetails(
            instanceTypes = (overrideTypes ++ runningTypes).distinct,
            launchTemplate = launchTemplate,
            instanceCount = group.instances.asScala.size,
            desiredCapacity =
              Option(group.desiredCapacity).map(_.intValue).getOrElse(0),
            maxSize = Option(group.maxSize).map(_.intValue).getOrElse(0)
          )
        }
    }

  private def launchTemplateInstanceType(
      ec2: Ec2Client,
      ref: LaunchTemplateRef
  ): IO[Option[String]] =
    IO.interruptible {
      val builder = DescribeLaunchTemplateVersionsRequest.builder
      val identified = (ref.id, ref.name) match {
        case (Some(id), _)      => Some(builder.launchTemplateId(id))
        case (None, Some(name)) => Some(builder.launchTemplateName(name))
        case (None, None)       => None
      }
      identified.flatMap { withTemplate =>
        val request = withTemplate
          .versions(ref.version.getOrElse(defaultLaunchTemplateVersion))
          .build
        ec2
          .describeLaunchTemplateVersions(request)
          .launchTemplateVersions
          .asScala
          .toList
          .headOption
          .flatMap(version => Option(version.launchTemplateData))
          .flatMap(data => Option(data.instanceTypeAsString))
          .filter(_.nonEmpty)
      }
    }

  private def describeInstanceTypeCapacities(
      ec2: Ec2Client,
      cache: Ref[IO, Map[String, InstanceTypeCapacity]],
      names: List[String]
  ): IO[Map[String, InstanceTypeCapacity]] = {
    val unique = names.distinct
    if (unique.isEmpty) IO.pure(Map.empty)
    else
      cache.get.flatMap { cached =>
        val missing = unique.filterNot(cached.contains)
        if (missing.isEmpty)
          IO.pure(cached.view.filterKeys(unique.toSet).toMap)
        else
          IO.interruptible {
            val response = ec2.describeInstanceTypes(
              DescribeInstanceTypesRequest.builder
                .instanceTypesWithStrings(missing.asJava)
                .build
            )
            response.instanceTypes.asScala.toList.map { instanceType =>
              val gpuCount = Option(instanceType.gpuInfo)
                .map(_.gpus.asScala.toList.map(_.count.intValue).sum)
                .getOrElse(0)
              instanceType.instanceTypeAsString -> InstanceTypeCapacity(
                instanceType.vCpuInfo.defaultVCpus,
                instanceType.memoryInfo.sizeInMiB.toInt,
                gpuCount
              )
            }.toMap
          }.flatMap { fetched =>
            cache
              .update(_ ++ fetched)
              .as((cached ++ fetched).view.filterKeys(unique.toSet).toMap)
          }
      }
  }

  private[ecs] def describeCapacityProviderInfo(
      ecs: EcsClient,
      autoscaling: AutoScalingClient,
      ec2: Ec2Client,
      cache: Ref[IO, Map[String, InstanceTypeCapacity]],
      capacityProvider: String
  ): IO[CapacityProviderInfo] =
    describeCapacityProvider(ecs, capacityProvider).flatMap {
      case None =>
        IO.raiseError(
          new RuntimeException(
            s"ECS knows no capacity provider named '$capacityProvider'. It may " +
              "be misspelled, may have been deleted, or may live in another " +
              "region. Fix EcsConfig.capacityProviders."
          )
        )
      case Some(provider) if provider.autoScalingGroupArn.isEmpty =>
        IO(
          scribe.debug(
            s"ECS capacity provider $capacityProvider is not backed by an " +
              "auto scaling group; its instance shapes are unknown and every " +
              "worker request will be offered to it"
          )
        ).as(unknownFit(capacityProvider))
      case Some(provider) =>
        val arn = provider.autoScalingGroupArn.get
        autoScalingGroupName(arn) match {
          case None =>
            IO.raiseError(
              new RuntimeException(
                s"The auto scaling group of ECS capacity provider " +
                  s"'$capacityProvider' is '$arn', which carries no group name."
              )
            )
          case Some(groupName) =>
            describeAutoScalingGroup(autoscaling, groupName).flatMap {
              case None =>
                IO.raiseError(
                  new RuntimeException(
                    s"ECS capacity provider '$capacityProvider' names auto " +
                      s"scaling group '$groupName', which does not exist."
                  )
                )
              case Some(details) =>
                val names =
                  if (details.instanceTypes.nonEmpty)
                    IO.pure(details.instanceTypes)
                  else
                    details.launchTemplate.fold(IO.pure(List.empty[String]))(
                      ref => launchTemplateInstanceType(ec2, ref).map(_.toList)
                    )
                names
                  .flatMap(describeInstanceTypeCapacities(ec2, cache, _))
                  .map(capacities =>
                    CapacityProviderInfo(
                      name = capacityProvider,
                      instanceTypes = capacities.values.toList,
                      canScaleOut = details.canScaleOut
                    )
                  )
            }
        }
    }

  private[ecs] def renderInstanceTypes(
      instanceTypes: List[InstanceTypeCapacity]
  ): String =
    if (instanceTypes.isEmpty) "unknown"
    else
      instanceTypes
        .map(t => s"[vcpu=${t.vcpus},memMiB=${t.memoryMib},gpu=${t.gpus}]")
        .mkString(", ")
}
