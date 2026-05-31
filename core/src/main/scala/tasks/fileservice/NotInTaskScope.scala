package tasks.fileservice

import tasks.queue.ComputationEnvironment

sealed trait NotInTaskScope

trait LowPriorityNotInTaskScope {
  implicit def default: NotInTaskScope = NotInTaskScope.witness
}

object NotInTaskScope extends LowPriorityNotInTaskScope {
  private[fileservice] val witness: NotInTaskScope = new NotInTaskScope {}

  @scala.annotation.implicitAmbiguous(
    "The legacy SharedFile(file, name) constructors are not allowed inside a task body, because their name argument is not scoped by the task input and may silently collide with other invocations of the same task. Use SharedFile.scoped(file, suffix) instead — it prefixes the path with the task's input-derived Prefix. To opt out in tests or legacy code, add: import tasks.fileservice.NotInTaskScope.allowInTaskScope._"
  )
  implicit def ambig1(implicit ev: ComputationEnvironment): NotInTaskScope = {
    val _ = ev
    witness
  }

  implicit def ambig2(implicit ev: ComputationEnvironment): NotInTaskScope = {
    val _ = ev
    witness
  }

  /** Opt out of the in-task-body restriction on the legacy
    * `SharedFile(file, name)` API. Bring into scope with:
    *
    * {{{
    * import tasks.fileservice.NotInTaskScope.allowInTaskScope._
    * }}}
    */
  object allowInTaskScope {
    implicit val allow: NotInTaskScope = witness
  }
}
