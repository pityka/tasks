package tasks.ecoll.ops

import tasks._
import scala.concurrent.Future

case class Partial[T, K](
    private val p: T => ResourceRequest => TaskSystemComponents => Future[K]) {
  def apply(d: T)(rr: ResourceRequest)(implicit tsc: TaskSystemComponents) =
    p(d)(rr)(tsc)
}
