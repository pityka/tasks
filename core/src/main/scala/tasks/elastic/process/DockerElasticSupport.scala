package tasks.elastic.process

import cats.effect._
import tasks.elastic.ElasticSupport
import tasks.shared.ResourceAllocated

final case class DockerConfig(
    image: Option[String],
    network: String,
    environment: Map[String, String],
    contexts: List[ProcessContext]
) extends ProcessConfig {

  val minimumResourceAllocation = true

  def withImage(value: String): DockerConfig = copy(image = Some(value))

  def withNetwork(value: String): DockerConfig = copy(network = value)

  def withEnvironment(entries: (String, String)*): DockerConfig =
    copy(environment = environment ++ entries)

  def withContext(value: ProcessContext): DockerConfig =
    copy(contexts = contexts :+ value)
}

object DockerConfig {

  val defaultImage = "eclipse-temurin:17.0.13_11-jre-ubi9-minimal"

  def apply(contexts: List[ProcessContext]): DockerConfig =
    DockerConfig(
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
