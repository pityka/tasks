package tasks.ecoll.ops

import tasks.ecoll._

import tasks.queue._
import tasks._
import akka.stream.scaladsl.Source
import akka.NotUsed

private[ecoll] object Distinct {

  def distinctSpore[A] =
    spore[
      (
          Source[A, NotUsed],
          Option[Nothing],
          ComputationEnvironment,
          SerDe[A],
          SerDe[Nothing]
      ),
      Source[A, NotUsed]
    ] {
      case (source: Source[A, NotUsed], _, _, _, _) =>
        source.statefulMapConcat(() => {
          var last: Option[A] = None
          (elem: A) =>
            {
              if (last.isDefined && elem == last.get) Nil
              else {
                last = Some(elem)
                List(elem)
              }
            }
        })
    }

}

trait DistinctOps {

  def distinct[A: SerDe](
      taskID: String,
      taskVersion: Int,
      outName: Option[String] = None
  ): Partial[EColl[A], EColl[A]] =
    Partial({
      case data1 =>
        resourceRequest => tsc =>
          val inner = Distinct.distinctSpore[A]

          GenericMap.task(taskID, taskVersion)(
            GenericMap.Input[A, Nothing, A](
              data1,
              None,
              implicitly[SerDe[A]],
              SerDe.nothing,
              implicitly[SerDe[A]],
              None,
              inner,
              false,
              outName,
              taskID,
              taskVersion
            )
          )(resourceRequest)(tsc)
    })

}
