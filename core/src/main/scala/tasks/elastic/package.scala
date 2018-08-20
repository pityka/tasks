package tasks

import tasks.util.config.TasksConfig

package object elastic {
  def elasticSupport(implicit config: TasksConfig) = config.gridEngine match {
    case "EC2"      => Some(tasks.elastic.ec2.EC2Grid)
    case "SSH"      => Some(tasks.elastic.ssh.SSHGrid)
    case "SH"       => Some(tasks.elastic.sh.SHGrid)
    case ""         => None
    case "NOENGINE" => None
    case reflective =>
      type T = tasks.elastic.NodeCreatorImpl
      type U = tasks.elastic.SelfShutdown
      Some(
        tasks.util
          .reflectivelyInstantiateObject[ElasticSupport[T, U]](reflective))

  }
}
