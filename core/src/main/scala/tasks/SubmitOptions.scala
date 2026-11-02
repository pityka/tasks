package tasks

import tasks.shared.{Priority, Labels}

sealed trait SubmitOptions {
  def priorityBase: Priority
  def labels: Labels
  def noCache: Boolean
  def withPriorityBase(p: Priority): SubmitOptions
  def withLabels(l: Labels): SubmitOptions
  def withNoCache(n: Boolean): SubmitOptions
}

object SubmitOptions {
  private[tasks] final case class SubmitOptionsImpl(
      priorityBase: Priority,
      labels: Labels,
      noCache: Boolean
  ) extends SubmitOptions {
    def withPriorityBase(p: Priority): SubmitOptions = copy(priorityBase = p)
    def withLabels(l: Labels): SubmitOptions = copy(labels = l)
    def withNoCache(n: Boolean): SubmitOptions = copy(noCache = n)
  }

  val default: SubmitOptions = SubmitOptionsImpl(Priority(0), Labels.empty, false)
}
