package tasks.elastic.process

import cats.effect._
import tasks.elastic.ElasticSupport
import tasks.shared.ResourceAllocated

sealed trait ShellConfig extends ProcessConfig {
  def withContext(value: ProcessContext): ShellConfig
}

object ShellConfig {
  private[tasks] final case class ShellConfigImpl(
      contexts: List[ProcessContext]
  ) extends ShellConfig {

    val minimumResourceAllocation = false

    def withContext(value: ProcessContext): ShellConfig =
      copy(contexts = contexts :+ value)
  }

  def apply(contexts: List[ProcessContext]): ShellConfig =
    ShellConfigImpl(contexts)

  def unapply(c: ShellConfig): Some[List[ProcessContext]] =
    Some(c.contexts)
}

object LocalShellElasticSupport {

  private object SHShutdownCommand extends RemoteShutdownCommand {
    def apply(contextName: String, processId: ProcessId): List[String] =
      List(
        "kill",
        processId.s
      )
  }
  private object SHSpawnProcessCommand extends SpawnProcessCommand {
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

  def make(shellConfig: ShellConfig): IO[ElasticSupport] =
    ProcessElasticSupport.make(
      processConfig = shellConfig,
      shutdownCommand = SHShutdownCommand,
      spawnProcessCommand = SHSpawnProcessCommand
    )
}
