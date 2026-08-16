package tasks.elastic.ecs

import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain

final case class EcsConfig(
    region: Option[String],
    cluster: String,
    capacityProvider: String,
    capacityProviderBase: Int,
    capacityProviderWeight: Int,
    containerName: String,
    taskDefinition: String,
    taskDefinitionsByImage: Map[String, String],
    minimumCpu: Int,
    minimumMemory: Int,
    startedBy: String,
    stopReason: String,
    extraEnvironment: Map[String, String],
    tags: Map[String, String]
) {

  require(
    cluster.nonEmpty,
    "cluster must be a non-empty ECS cluster name or ARN"
  )

  require(
    capacityProvider.nonEmpty,
    s"capacityProvider must name a capacity provider already associated " +
      s"with $cluster"
  )

  require(
    containerName.nonEmpty,
    "containerName must name the container inside the task definition " +
      "whose command is overridden with the worker bootstrap script"
  )

  require(
    taskDefinition.nonEmpty,
    "taskDefinition must be a non-empty task definition family, " +
      "family:revision or ARN"
  )

  require(
    startedBy.nonEmpty && startedBy.length <= 36,
    "startedBy must be non-empty and at most 36 characters (ECS limit)"
  )

  def withRegion(value: String): EcsConfig = copy(region = Some(value))

  def withCapacityProviderStrategy(base: Int, weight: Int): EcsConfig =
    copy(capacityProviderBase = base, capacityProviderWeight = weight)

  def withTaskDefinitionForImage(image: String, value: String): EcsConfig =
    copy(taskDefinitionsByImage = taskDefinitionsByImage + (image -> value))

  def withMinimumResources(cpu: Int, memoryMib: Int): EcsConfig =
    copy(minimumCpu = cpu, minimumMemory = memoryMib)

  def withStartedBy(value: String): EcsConfig = copy(startedBy = value)

  def withStopReason(value: String): EcsConfig = copy(stopReason = value)

  def withEnvironment(entries: (String, String)*): EcsConfig =
    copy(extraEnvironment = extraEnvironment ++ entries)

  def withTags(entries: (String, String)*): EcsConfig =
    copy(tags = tags ++ entries)

  def resolveTaskDefinition(image: Option[String]): Either[String, String] =
    image match {
      case None => Right(taskDefinition)
      case Some(img) =>
        taskDefinitionsByImage
          .get(img)
          .toRight(
            s"No ECS task definition configured for image '$img'. " +
              "Register one with EcsConfig.withTaskDefinitionForImage. " +
              "ECS takes the image from the task definition, so it cannot be " +
              "overridden at RunTask time."
          )
    }

  override def toString: String =
    s"EcsConfig(region=${region.getOrElse("<default-chain>")}, " +
      s"cluster=$cluster, capacityProvider=$capacityProvider, " +
      s"taskDefinition=$taskDefinition, container=$containerName, " +
      s"imagesMapped=${taskDefinitionsByImage.size})"
}

object EcsConfig {

  def apply(
      cluster: String,
      capacityProvider: String,
      containerName: String,
      taskDefinition: String
  ): EcsConfig =
    EcsConfig(
      region = None,
      cluster = cluster,
      capacityProvider = capacityProvider,
      capacityProviderBase = 0,
      capacityProviderWeight = 1,
      containerName = containerName,
      taskDefinition = taskDefinition,
      taskDefinitionsByImage = Map.empty,
      minimumCpu = 1,
      minimumMemory = 512,
      startedBy = "tasks-elastic",
      stopReason = "Shut down by tasks framework",
      extraEnvironment = Map.empty,
      tags = Map.empty
    )

  def resolveRegion(configured: Option[String]): String =
    configured
      .filter(_.nonEmpty)
      .getOrElse(
        scala.util
          .Try(
            DefaultAwsRegionProviderChain.builder().build().getRegion().id()
          )
          .toOption
          .filter(_.nonEmpty)
          .getOrElse(
            throw new RuntimeException(
              "No AWS region for the ECS backend: set EcsConfig.withRegion, " +
                "AWS_REGION, or configure a profile with a default region."
            )
          )
      )
}
