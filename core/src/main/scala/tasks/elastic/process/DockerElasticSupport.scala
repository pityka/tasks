package tasks.elastic.process

import org.ekrich.config.Config
import cats.effect._
import tasks.elastic.ElasticSupport
import scala.jdk.CollectionConverters._
import tasks.shared.ResourceAllocated

object DockerElasticSupport {

  private class DockerConfig(raw: Config) extends ProcessConfig {
    val dockerImageName = raw.getString("tasks.docker.image")
    val minimumResourceAllocation = true

    val dockerEnvVars =
      raw.getStringList("tasks.docker.env").asScala.grouped(2).map { g =>
        (g(0), g(1))
      }
    val contexts = raw.getConfigList("tasks.docker.contexts").asScala.toList
    val dockerNetwork = raw.getString("tasks.docker.network")
  }

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
        s"--network=${config.dockerNetwork}"
      ) ++ config.dockerEnvVars.toList.flatMap { case (k, v) =>
        List("--env", s"$k=$v")
      } ++ (if (allocated.gpu.nonEmpty)
              List("--gpus", allocated.gpu.mkString(","))
            else Nil) ++
        List(
          allocated.image
            .orElse(Option(config.dockerImageName).filter(_.nonEmpty))
            .getOrElse("eclipse-temurin:17.0.13_11-jre-ubi9-minimal"),
          "/bin/bash",
          "-c",
          script
        )

  }

  def make(config: Option[Config]): IO[ElasticSupport] = {
    IO(tasks.util.loadConfig(config)).flatMap { config =>
      val dockerConfig = new DockerConfig(config)
      ProcessElasticSupport.make(
        processConfig = dockerConfig,
        shutdownCommand = DockerShutdownCommand,
        spawnProcessCommand = new DockerSpawnProcessCommand(dockerConfig)
      )
    }
  }
}
