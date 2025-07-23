package tasks.elastic.process

import org.ekrich.config.Config
import cats.effect._
import tasks.elastic.ElasticSupport
import scala.jdk.CollectionConverters._
import tasks.shared.ResourceAllocated

object LocalShellElasticSupport {

  private class PConfig(raw: Config) extends ProcessConfig {
    val minimumResourceAllocation = false
    val contexts = raw.getConfigList("tasks.sh.contexts").asScala.toList
  }

  private object SHShutdownCommand extends RemoteShutdownCommand {
    def apply(contextName: String, processId: ProcessId): List[String] =
      List(
        "kill",
        processId.s
      )
  }
  private class SHSpawnProcessCommand(config: PConfig)
      extends SpawnProcessCommand {
    val background = true
    override def apply(
        context: String,
        allocated: ResourceAllocated,
        script: String
    ): List[String] =
      List(
        "bash",
        "-c",
        script
      )

  }

  def make(config: Option[Config]): IO[ElasticSupport] = {
    IO(tasks.util.loadConfig(config)).flatMap { config =>
      val c = new PConfig(config)
      ProcessElasticSupport.make(
        processConfig = c,
        shutdownCommand = SHShutdownCommand,
        spawnProcessCommand = new SHSpawnProcessCommand(c)
      )
    }
  }
}
