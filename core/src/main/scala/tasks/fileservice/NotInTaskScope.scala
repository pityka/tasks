package tasks.fileservice

import tasks.TaskSystemComponents

sealed trait NotInTaskScope

trait LowPriorityNotInTaskScope {
  implicit def default: NotInTaskScope = NotInTaskScope.witness
}

object NotInTaskScope extends LowPriorityNotInTaskScope {
  private[fileservice] val witness: NotInTaskScope = new NotInTaskScope {}

  @scala.annotation.implicitAmbiguous(
    "The legacy SharedFile(file, name) constructors are not allowed when a TaskSystemComponents is in implicit scope, because their name argument is not scoped by the task input and may silently collide with other invocations of the same task. Use SharedFile.scoped(file, suffix) instead — it prefixes the path with the task's input-derived Prefix. To opt out add: import tasks.fileservice.allowUnscopedSharedFiles.allow"
  )
  implicit def ambig1(implicit ev: TaskSystemComponents): NotInTaskScope = {
    val _ = ev
    witness
  }

  implicit def ambig2(implicit ev: TaskSystemComponents): NotInTaskScope = {
    val _ = ev
    witness
  }

}

/** Opt out of the in-task-body restriction on the legacy `SharedFile(file,
  * name)` API. Bring into scope with:
  *
  * {{{
  * import tasks.fileservice.allowUnscopedSharedFiles.allow
  * }}}
  */
object allowUnscopedSharedFiles {
  implicit val allow: NotInTaskScope = NotInTaskScope.witness
}
