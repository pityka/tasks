package tasks.ecoll.ops

import tasks.ecoll._

import tasks.queue._
import tasks._
import akka.stream.scaladsl.Source
import akka.NotUsed

private[ecoll] object Fold {

  def foldSpore[A, B](fun: Spore[(B, A), B]) =
    spore[(Source[A, NotUsed], B, ComputationEnvironment, SerDe[A], SerDe[B]),
          Source[B, NotUsed]] {
      case (source: Source[A, NotUsed], const, _, _, _) =>
        source.fold(const)((b, a) => fun.apply((b, a)))
    }

  def foldSporeFromOption[A, B](fun: Spore[(B, A), B]) =
    spore[(Source[A, NotUsed],
           Option[B],
           ComputationEnvironment,
           SerDe[A],
           SerDe[B]),
          Source[B, NotUsed]] {
      case (source: Source[A, NotUsed], const: Option[B], _, _, _) =>
        source.fold(const.get)((b, a) => fun.apply((b, a)))
    }

}

trait FoldOps {

  def foldConstant[A: SerDe, B: SerDe](
      taskID: String,
      taskVersion: Int,
      zero: B)(fun: Spore[(B, A), B]): Partial[EColl[A], EColl[B]] =
    Partial({
      case data1 =>
        resourceRequest => tsc =>
          val inner = Fold.foldSpore[A, B](fun)

          val partial = EColl.mapWith(taskID, taskVersion, false)(inner)
          partial((data1, zero))(resourceRequest)(tsc)
    })

  def fold[A: SerDe, B: SerDe](taskID: String, taskVersion: Int)(
      fun: Spore[(B, A), B]): Partial[(EColl[A], B), EColl[B]] =
    Partial({
      case (data1, data2) =>
        resourceRequest => tsc =>
          val inner = Fold.foldSpore[A, B](fun)

          val partial = EColl.mapWith(taskID, taskVersion, false)(inner)
          partial((data1, data2))(resourceRequest)(tsc)
    })

  def foldWithFirst[A: SerDe, B: SerDe](taskID: String,
                                        taskVersion: Int,
                                        outName: Option[String] = None)(
      fun: Spore[(B, A), B]): Partial[(EColl[A], EColl[B]), EColl[B]] =
    Partial({
      case (data1, data2) =>
        resourceRequest => tsc =>
          val inner = Fold.foldSporeFromOption[A, B](fun)

          GenericMap.task(taskID, taskVersion)(
            GenericMap.Input[A, B, B](data1,
                                      Some(data2),
                                      implicitly[SerDe[A]],
                                      implicitly[SerDe[B]],
                                      implicitly[SerDe[B]],
                                      None,
                                      inner,
                                      false,
                                      outName,
                                      taskID,
                                      taskVersion))(resourceRequest)(tsc)
    })

}
