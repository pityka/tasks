package tasks.ecoll.ops

import tasks.ecoll._

import tasks.queue._
import tasks._
import akka.stream.scaladsl.Source
import akka.NotUsed

private[ecoll] object MapConcat {

  def mapConcatSpore[A, B](fun: Spore[A, Seq[B]]) =
    spore[(Source[A, NotUsed],
           Option[Nothing],
           ComputationEnvironment,
           SerDe[A],
           SerDe[Nothing]),
          Source[B, NotUsed]] {
      case (source: Source[A, NotUsed], _: Option[Nothing], _, _, _) =>
        source.mapConcat(a => fun.apply(a).toList)
    }

}

trait MapConcatOps {

  def mapConcat[A: SerDe, B: SerDe](taskID: String,
                                    taskVersion: Int,
                                    outName: Option[String] = None)(
      fun: Spore[A, Seq[B]]): Partial[EColl[A], EColl[B]] =
    Partial({
      case data1 =>
        resourceRequest => tsc =>
          val inner = MapConcat.mapConcatSpore[A, B](fun)

          GenericMap.task(taskID, taskVersion)(
            GenericMap.Input[A, Nothing, B](data1,
                                            None,
                                            implicitly[SerDe[A]],
                                            SerDe.nothing,
                                            implicitly[SerDe[B]],
                                            None,
                                            inner,
                                            true,
                                            outName,
                                            taskID,
                                            taskVersion))(resourceRequest)(tsc)
    })

}
