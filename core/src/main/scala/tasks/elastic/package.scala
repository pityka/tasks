package tasks

import tasks.util.config.TasksConfig
import cats.effect._
package object elastic {
  def makeElasticSupport(implicit config: TasksConfig): Resource[IO,Option[ElasticSupport]] =
    config.elasticSupport match {
      case ""         => Resource.pure(None)
      case "NOENGINE" => Resource.pure(None)
      case reflective =>
          tasks.util
            .reflectivelyInstantiateObject[ElasticSupportFromConfig](reflective)
            .apply(config).map(Some(_))

    }
}
