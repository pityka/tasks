package tasks

import tasks.queue.ComputationEnvironment

/** Marker summoned by [[SubmitContext.fromTopLevel]] to ensure no
  * [[ComputationEnvironment]] is in implicit scope.
  *
  * On Scala 2 the two `ambiguous*` derivations are intentionally ambiguous
  * whenever a `ComputationEnvironment` is implicitly available, so implicit
  * search for `NotInLeaf` fails inside any task body. When no CE is around,
  * only `default` is a candidate and the marker resolves cleanly.
  */
sealed class NotInLeaf
object NotInLeaf {
  implicit def default: NotInLeaf = new NotInLeaf
  implicit def ambiguousIfLeaf1(implicit
      ce: ComputationEnvironment
  ): NotInLeaf = throw new AssertionError(
    "NotInLeaf ambiguity instance must never be summoned"
  )
  implicit def ambiguousIfLeaf2(implicit
      ce: ComputationEnvironment
  ): NotInLeaf = throw new AssertionError(
    "NotInLeaf ambiguity instance must never be summoned"
  )
}
