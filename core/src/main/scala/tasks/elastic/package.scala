package tasks

import tasks.util.config.TasksConfig

package object elastic {
  def makeElasticSupport(implicit config: TasksConfig): Option[ElasticSupport] =
    config.elasticSupport match {
      case ""         => None
      case "NOENGINE" => None
      case reflective =>
        Some(
          tasks.util
            .reflectivelyInstantiateObject[ElasticSupportFromConfig](reflective)
            .apply(config))

    }
}
