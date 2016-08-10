package tasks.elastic
import tasks.util.config

object TaskAllocationConstants {

  val LaunchWithDocker = if (config.global.hasPath("tasks.elastic.docker")) {
    Some(
        config.global
          .getString("tasks.elastic.docker.registry") -> config.global
          .getString("tasks.elastic.docker.image"))
  } else None

  val KillIdleNodeAfterSeconds =
    config.global.getLong("tasks.elastic.killIdleNodeAfterSeconds")

  val MaxNodes = config.global.getInt("tasks.elastic.maxNodes")

  val MaxPendingNodes = config.global.getInt("tasks.elastic.maxPending")

  val NewNodeJarPath =
    config.global.getString("tasks.elastic.mainClassWithClassPathOrJar")

  val QueueCheckInterval =
    config.global.getInt("tasks.elastic.queueCheckInterval")

  val QueueCheckInitialDelay =
    config.global.getInt("tasks.elastic.queueCheckInitialDelay")

  val NodeKillerMonitorInterval =
    config.global.getInt("tasks.elastic.nodeKillerMonitorInterval")

  val JVMMaxHeapFactor =
    config.global.getDouble("tasks.elastic.jvmMaxHeapFactor")

  val logQueueStatus = config.global.getBoolean("tasks.elastic.logQueueStatus")

  val AdditionalSystemProperties: List[String] = config.global
    .getStringList("tasks.elastic.additionalSystemProperties")
    .toArray
    .map(_.asInstanceOf[String])
    .toList

}
