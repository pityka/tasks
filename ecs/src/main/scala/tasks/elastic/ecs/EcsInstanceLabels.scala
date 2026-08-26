package tasks.elastic.ecs

import cats.effect.IO
import scala.jdk.CollectionConverters._
import software.amazon.awssdk.services.ecs.EcsClient
import software.amazon.awssdk.services.ecs.model.DescribeContainerInstancesRequest
import software.amazon.awssdk.services.ecs.model.DescribeTasksRequest

object EcsInstanceLabels {

  val builtInPrefix: String = "ecs."

  private[ecs] def advertised(
      attributes: List[(String, Option[String])],
      builtIns: Set[String]
  ): Set[String] =
    attributes.iterator
      .filter { case (name, _) =>
        !name.startsWith(builtInPrefix) || builtIns.contains(name)
      }
      .map { case (name, value) =>
        (EcsAttributes.labelOf(name, value), name, value)
      }
      .filter { case (label, name, value) =>
        EcsAttributes.attributeOf(label) == Right(
          (name, value.filter(_.nonEmpty))
        )
      }
      .map(_._1)
      .toSet

  def discover(ecs: EcsClient, ecsConfig: EcsConfig): IO[Set[String]] =
    discover(
      ecs,
      ecsConfig,
      Option(System.getenv(EcsGetNodeName.ecsMetadataUriVariable))
    )

  def discover(
      ecs: EcsClient,
      ecsConfig: EcsConfig,
      metadataUri: Option[String]
  ): IO[Set[String]] =
    metadataUri match {
      case None =>
        IO(
          scribe.warn(
            s"${EcsGetNodeName.ecsMetadataUriVariable} is not set, so this " +
              "process is not an ECS task and advertises no container " +
              "instance attributes as node selector labels."
          )
        ).as(Set.empty)
      case Some(uri) =>
        val lookup = for {
          taskArn <- EcsGetNodeName.fetchTaskArn(uri)
          containerInstanceArn <- containerInstanceOf(ecs, ecsConfig, taskArn)
          labels <- containerInstanceArn match {
            case None =>
              IO(
                scribe.warn(
                  s"The ECS task $taskArn runs on no container instance, so " +
                    "it advertises no container instance attributes as node " +
                    "selector labels. This is expected on Fargate."
                )
              ).as(Set.empty[String])
            case Some(arn) =>
              attributesOf(ecs, ecsConfig, arn).map(
                advertised(_, ecsConfig.advertisedBuiltInAttributes)
              )
          }
        } yield labels

        lookup
          .flatTap(labels =>
            IO(
              scribe.info(
                s"Advertising ${labels.size} ECS container instance " +
                  s"attribute(s) as node selector labels: " +
                  labels.toList.sorted.mkString(", ")
              )
            )
          )
          .handleErrorWith { e =>
            IO(
              scribe.warn(
                "Failed to read the attributes of the ECS container instance " +
                  "this worker runs on, so it advertises only the labels in " +
                  "hosts.labels and hosts.labelsAsCommaString. This matters " +
                  "only for tasks submitted with a node selector, which will " +
                  "not be scheduled on this worker. The lookup needs " +
                  "ecs:DescribeTasks and ecs:DescribeContainerInstances on " +
                  "the task role. Disable it with " +
                  "EcsConfig.withoutInstanceAttributeAdvertisement.",
                e
              )
            ).as(Set.empty[String])
          }
    }

  private def containerInstanceOf(
      ecs: EcsClient,
      ecsConfig: EcsConfig,
      taskArn: String
  ): IO[Option[String]] =
    IO.interruptible {
      ecs
        .describeTasks(
          DescribeTasksRequest.builder
            .cluster(ecsConfig.cluster)
            .tasks(taskArn)
            .build
        )
        .tasks
        .asScala
        .toList
        .headOption
        .flatMap(task => Option(task.containerInstanceArn))
        .filter(_.nonEmpty)
    }

  private def attributesOf(
      ecs: EcsClient,
      ecsConfig: EcsConfig,
      containerInstanceArn: String
  ): IO[List[(String, Option[String])]] =
    IO.interruptible {
      ecs
        .describeContainerInstances(
          DescribeContainerInstancesRequest.builder
            .cluster(ecsConfig.cluster)
            .containerInstances(containerInstanceArn)
            .build
        )
        .containerInstances
        .asScala
        .toList
        .headOption
        .toList
        .flatMap(_.attributes.asScala.toList)
        .flatMap { attribute =>
          Option(attribute.name)
            .filter(_.nonEmpty)
            .map(name => (name, Option(attribute.value).filter(_.nonEmpty)))
        }
    }
}
