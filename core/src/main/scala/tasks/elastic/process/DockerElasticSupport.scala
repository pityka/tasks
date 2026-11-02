package tasks.elastic.process

import cats.effect._
import tasks.elastic.ElasticSupport
import tasks.shared.ResourceAllocated

sealed trait DockerConfig extends ProcessConfig {
  def image: Option[String]
  def network: String
  def environment: Map[String, String]
  def contexts: List[ProcessContext]

  final val minimumResourceAllocation = true

  final def withImage(value: String): DockerConfig =
    DockerConfig(Some(value), network, environment, contexts)

  final def withNetwork(value: String): DockerConfig =
    DockerConfig(image, value, environment, contexts)

  final def withEnvironment(entries: (String, String)*): DockerConfig =
    DockerConfig(image, network, environment ++ entries, contexts)

  final def withContext(value: ProcessContext): DockerConfig =
    DockerConfig(image, network, environment, contexts :+ value)
}

object DockerConfig {

  private[tasks] final case class DockerConfigImpl(
      image: Option[String],
      network: String,
      environment: Map[String, String],
      contexts: List[ProcessContext]
  ) extends DockerConfig

  val defaultImage = "eclipse-temurin:17.0.13_11-jre-ubi9-minimal"

  def apply(
      image: Option[String],
      network: String,
      environment: Map[String, String],
      contexts: List[ProcessContext]
  ): DockerConfig =
    DockerConfigImpl(image, network, environment, contexts)

  def apply(contexts: List[ProcessContext]): DockerConfig =
    DockerConfigImpl(
      image = None,
      network = "host",
      environment = Map.empty,
      contexts = contexts
    )
}

object DockerElasticSupport {

  private object DockerShutdownCommand extends RemoteShutdownCommand {
    def apply(contextName: String, processId: ProcessId): List[String] =
      List(
        "docker",
        "--context",
        contextName,
        "kill",
        processId.s
      )
  }
  private class DockerSpawnProcessCommand(config: DockerConfig)
      extends SpawnProcessCommand {
    val background = false
    override def apply(
        context: String,
        allocated: ResourceAllocated,
        script: String
    ): List[String] =
      List(
        "docker",
        "--context",
        context,
        "run",
        "-d",
        s"--network=${config.network}"
      ) ++ config.environment.toList.flatMap { case (k, v) =>
        List("--env", s"$k=$v")
      } ++ (if (allocated.gpu.nonEmpty)
              List("--gpus", allocated.gpu.mkString(","))
            else Nil) ++
        List(
          allocated.image
            .orElse(config.image.filter(_.nonEmpty))
            .getOrElse(DockerConfig.defaultImage),
          "/bin/bash",
          "-c",
          script
        )

  }

  def make(dockerConfig: DockerConfig): IO[ElasticSupport] =
    ProcessElasticSupport.make(
      processConfig = dockerConfig,
      shutdownCommand = DockerShutdownCommand,
      spawnProcessCommand = new DockerSpawnProcessCommand(dockerConfig)
    )
}
