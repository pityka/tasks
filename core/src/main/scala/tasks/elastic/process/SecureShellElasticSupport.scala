package tasks.elastic.process

import org.ekrich.config.Config
import cats.effect._
import tasks.elastic.ElasticSupport
import scala.jdk.CollectionConverters._
import tasks.shared.ResourceAllocated

object SecureShellElasticSupport {

  private class PConfig(raw: Config) extends ProcessConfig {

    val minimumResourceAllocation = false

    val envVars =
      raw.getStringList("tasks.ssh.env").asScala.grouped(2).map { g =>
        (g(0), g(1))
      }
    val contexts = raw.getConfigList("tasks.ssh.contexts").asScala.toList
  }

  private object SSHShutdownCommand extends RemoteShutdownCommand {
    def apply(contextName: String, processId: ProcessId): List[String] =
      List(
        "ssh",
        contextName,
        "kill",
        processId.s
      )
  }
  private class SSHSpawnProcessCommand(config: PConfig)
      extends SpawnProcessCommand {

    val background = true

    override def apply(
        context: String,
        allocated: ResourceAllocated,
        script: String
    ): List[String] = {
      val scriptWithEnv = s"${config.envVars.toList
          .map { case (k, v) =>
            s"$k=$v"
          }
          .mkString("\n")} $script"
      List(
        "ssh",
        context,
        scriptWithEnv
      )
    }

  }

  def make(config: Option[Config]): IO[ElasticSupport] = {
    IO(tasks.util.loadConfig(config)).flatMap { config =>
      val c = new PConfig(config)
      ProcessElasticSupport.make(
        processConfig = c,
        shutdownCommand = SSHShutdownCommand,
        spawnProcessCommand = new SSHSpawnProcessCommand(c)
      )
    }
  }
}
