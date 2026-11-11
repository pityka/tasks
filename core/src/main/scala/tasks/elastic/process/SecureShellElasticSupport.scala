package tasks.elastic.process

import cats.effect._
import tasks.elastic.ElasticSupport
import tasks.shared.ResourceAllocated

sealed trait SecureShellConfig extends ProcessConfig {
  def environment: Map[String, String]

  def withEnvironment(entries: (String, String)*): SecureShellConfig
  def withContext(value: ProcessContext): SecureShellConfig
}

object SecureShellConfig {

  private[tasks] final case class SecureShellConfigImpl(
      environment: Map[String, String],
      contexts: List[ProcessContext]
  ) extends SecureShellConfig {

    val minimumResourceAllocation = false

    def withEnvironment(entries: (String, String)*): SecureShellConfig =
      copy(environment = environment ++ entries)

    def withContext(value: ProcessContext): SecureShellConfig =
      copy(contexts = contexts :+ value)
  }

  def apply(
      environment: Map[String, String],
      contexts: List[ProcessContext]
  ): SecureShellConfig =
    SecureShellConfigImpl(environment, contexts)

  def apply(contexts: List[ProcessContext]): SecureShellConfig =
    SecureShellConfigImpl(environment = Map.empty, contexts = contexts)
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
