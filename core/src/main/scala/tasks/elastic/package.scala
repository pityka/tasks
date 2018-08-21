package tasks

import tasks.util.config.TasksConfig

package object elastic {
  def makeElasticSupport(implicit config: TasksConfig) =
    config.gridEngine match {
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
