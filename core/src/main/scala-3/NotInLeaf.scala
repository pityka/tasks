package tasks

import tasks.queue.ComputationEnvironment

/** Marker summoned by [[SubmitContext.fromTopLevel]] to ensure no
  * [[ComputationEnvironment]] is in implicit scope.
  *
  * On Scala 3 we use `scala.util.NotGiven` directly: the marker is derivable
  * iff no implicit `ComputationEnvironment` is in scope.
  */
sealed class NotInLeaf
object NotInLeaf {
  implicit def fromAbsentCE(implicit
      ng: scala.util.NotGiven[ComputationEnvironment]
  ): NotInLeaf = new NotInLeaf
}
