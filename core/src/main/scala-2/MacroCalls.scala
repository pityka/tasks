package tasks

import scala.language.experimental.macros
import tasks.queue._
import cats.effect.IO
trait MacroCalls {
  def Task[A <: AnyRef, C](taskID: String, taskVersion: Int)(
      comp: A => ComputationEnvironment => IO[C]
  ): TaskDefinition[A, C] =
    macro TaskDefinitionMacros
      .taskDefinitionFromTree[A, C]

  def spore[A, B](value: A => B): Spore[A, B] =
    macro tasks.queue.SporeMacros
      .sporeImpl[A, B]

  def spore[B](value: () => B): Spore[Unit, B] =
    macro tasks.queue.SporeMacros
      .sporeImpl0[B]

  def makeSerDe[A]: SerDe[A] = macro SerdeMacro.create[A]
}
