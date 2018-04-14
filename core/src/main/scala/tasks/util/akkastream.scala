package tasks.util

import akka.stream.scaladsl.Flow
import scala.concurrent.duration._

object AkkaStreamComponents {
  def strictBatchWeighted[T](
      maxWeight: Long,
      cost: T => Long,
      maxDuration: FiniteDuration = 1 seconds
  )(reduce: (T, T) => T): Flow[T, T, _] =
    Flow[T]
      .groupedWeightedWithin(maxWeight, maxDuration)(cost)
      .map(_.reduce(reduce))
}
