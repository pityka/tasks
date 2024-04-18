package tasks

import scala.language.experimental.macros
import tasks.queue._
import cats.effect.IO
trait MacroCalls {
  inline def Task[A <: AnyRef, C](taskID: String, taskVersion: Int)(
      comp: A => ComputationEnvironment => IO[C]
  )(implicit
      inline serA: Serializer[A],
      inline deserA: Deserializer[A],
      inline serC: Serializer[C],
      inline deserC: Deserializer[C]
  ): TaskDefinition[A, C] = ${
    TaskDefinitionMacros
      .taskDefinitionFromTree[A, C](
        serA = 'serA,
        deserA = 'deserA,
        serC = 'serC,
        deserC = 'deserC,
        taskID = 'taskID,
        taskVersion = 'taskVersion,
        comp = 'comp
      )
  }

  inline def spore[A, B](value: A => B): Spore[A, B] =
    tasks.queue.SporeMacros
      .spore(value)

  inline def makeSerDe[A](implicit
      inline ser: Serializer[A],
      inline des: Deserializer[A]
  ) = ${
    SerdeMacro.create[A]('des, 'ser)
  }

}
