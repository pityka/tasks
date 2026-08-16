package tasks.elastic.process

import cats.effect._
import tasks.elastic.ElasticSupport
import tasks.shared.ResourceAllocated

final case class SecureShellConfig(
    environment: Map[String, String],
    contexts: List[ProcessContext]
) extends ProcessConfig {

  val minimumResourceAllocation = false

  def withEnvironment(entries: (String, String)*): SecureShellConfig =
    copy(environment = environment ++ entries)

  def withContext(value: ProcessContext): SecureShellConfig =
    copy(contexts = contexts :+ value)
}

object SecureShellConfig {

  def apply(contexts: List[ProcessContext]): SecureShellConfig =
    SecureShellConfig(environment = Map.empty, contexts = contexts)
}

object SecureShellElasticSupport {

  private object SSHShutdownCommand extends RemoteShutdownCommand {
    def apply(contextName: String, processId: ProcessId): List[String] =
      List(
        "ssh",
        contextName,
        "kill",
        processId.s
      )
  }
  private class SSHSpawnProcessCommand(config: SecureShellConfig)
      extends SpawnProcessCommand {

    val background = true

    override def apply(
        context: String,
        allocated: ResourceAllocated,
        script: String
    ): List[String] = {
      val scriptWithEnv = s"${config.environment.toList
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

  def make(sshConfig: SecureShellConfig): IO[ElasticSupport] =
    ProcessElasticSupport.make(
      processConfig = sshConfig,
      shutdownCommand = SSHShutdownCommand,
      spawnProcessCommand = new SSHSpawnProcessCommand(sshConfig)
    )
}
