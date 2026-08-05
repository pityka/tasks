package tasks.elastic.ecs

import scala.jdk.CollectionConverters._
import org.ekrich.config.Config
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import tasks.util.config.ConfigValuesForHostConfiguration

final class EcsConfig(val raw: Config) extends ConfigValuesForHostConfiguration {

  private val root = "tasks.elastic.ecs"

  val configuredRegion: String = raw.getString(s"$root.region")

  val cluster: String = {
    val v = raw.getString(s"$root.cluster")
    require(v.nonEmpty, s"$root.cluster must be a non-empty ECS cluster name or ARN")
    v
  }

  val capacityProvider: String = {
    val v = raw.getString(s"$root.capacityProvider")
    require(
      v.nonEmpty,
      s"$root.capacityProvider must name a capacity provider already " +
        s"associated with $cluster"
    )
    v
  }

  val capacityProviderBase: Int = raw.getInt(s"$root.capacityProviderBase")

  val capacityProviderWeight: Int = raw.getInt(s"$root.capacityProviderWeight")

  val containerName: String = {
    val v = raw.getString(s"$root.containerName")
    require(
      v.nonEmpty,
      s"$root.containerName must name the container inside the task definition " +
        "whose command is overridden with the worker bootstrap script"
    )
    v
  }

  val taskDefinition: String = {
    val v = raw.getString(s"$root.taskDefinition")
    require(
      v.nonEmpty,
      s"$root.taskDefinition must be a non-empty task definition family, " +
        "family:revision or ARN"
    )
    v
  }

  val taskDefinitionsByImage: Map[String, String] = {
    val path = s"$root.taskDefinitionsByImage"
    val entries =
      if (raw.hasPath(path)) raw.getConfigList(path).asScala.toList else Nil
    entries.map { c =>
      c.getString("image") -> c.getString("taskDefinition")
    }.toMap
  }

  def resolveTaskDefinition(image: Option[String]): Either[String, String] =
    image match {
      case None => Right(taskDefinition)
      case Some(img) =>
        taskDefinitionsByImage
          .get(img)
          .toRight(
            s"No ECS task definition configured for image '$img'. " +
              s"Register one under $root.taskDefinitionsByImage. " +
              "ECS takes the image from the task definition, so it cannot be " +
              "overridden at RunTask time."
          )
    }

  val minimumCpu: Int = raw.getInt(s"$root.minimumCpu")

  val minimumMemory: Int = raw.getInt(s"$root.minimumMemory")

  val startedBy: String = {
    val v = raw.getString(s"$root.startedBy")
    require(
      v.nonEmpty && v.length <= 36,
      s"$root.startedBy must be non-empty and at most 36 characters (ECS limit)"
    )
    v
  }

  val extraEnvironment: Map[String, String] = {
    val path = s"$root.environment"
    val list =
      if (raw.hasPath(path)) raw.getStringList(path).asScala.toList else Nil
    require(
      list.size % 2 == 0,
      s"$path must be a flat list of alternating keys and values"
    )
    list.grouped(2).collect { case k :: v :: Nil => k -> v }.toMap
  }

  val tags: Map[String, String] = {
    val list = raw.getStringList(s"$root.tags").asScala.toList
    require(
      list.size % 2 == 0,
      s"$root.tags must be a flat list of alternating keys and values"
    )
    list.grouped(2).collect { case k :: v :: Nil => k -> v }.toMap
  }

  val stopReason: String = raw.getString(s"$root.stopReason")

  override def toString: String =
    s"EcsConfig(region=${if (configuredRegion.isEmpty) "<default-chain>" else configuredRegion}, " +
      s"cluster=$cluster, capacityProvider=$capacityProvider, " +
      s"taskDefinition=$taskDefinition, container=$containerName, " +
      s"imagesMapped=${taskDefinitionsByImage.size})"
}

object EcsConfig {

  def resolveRegion(configured: String): String =
    if (configured.nonEmpty) configured
    else
      scala.util
        .Try(DefaultAwsRegionProviderChain.builder().build().getRegion().id())
        .toOption
        .filter(_.nonEmpty)
        .getOrElse(
          throw new RuntimeException(
            "No AWS region for the ECS backend: set tasks.elastic.ecs.region, " +
              "AWS_REGION, or configure a profile with a default region."
          )
        )
}
