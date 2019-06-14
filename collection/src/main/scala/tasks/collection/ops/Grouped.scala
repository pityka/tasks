package tasks.ecoll.ops

import tasks.ecoll._

import tasks.queue._
import tasks._
import akka.stream.scaladsl.Source
import akka.NotUsed

private[ecoll] object Grouped {

  def groupedSpore[A] =
    spore[(Source[A, NotUsed],
           Option[Int],
           ComputationEnvironment,
           SerDe[A],
           SerDe[Int]),
          Source[Seq[A], NotUsed]] {
      case (source: Source[A, NotUsed], n, _, _, _) =>
        source.grouped(n.get)
    }

}

trait GroupedOps {

  def groupedTotal[A: SerDe](taskID: String, taskVersion: Int)(
      implicit r: SerDe[Int],
      rs: SerDe[Seq[A]]): Partial[EColl[A], EColl[Seq[A]]] =
    Partial({
      case data1 =>
        resourceRequest => implicit tsc =>
          val g = grouped[A](taskID, taskVersion)
          g((data1, Int.MaxValue))(resourceRequest)(tsc)
    })

  def grouped[A: SerDe](taskID: String,
                        taskVersion: Int,
                        outName: Option[String] = None)(
      implicit r: SerDe[Int],
      rs: SerDe[Seq[A]]): Partial[(EColl[A], Int), EColl[Seq[A]]] =
    Partial({
      case (data1, n: Int) =>
        resourceRequest => implicit tsc =>
          implicit val ec = tsc.executionContext
          val inner = Grouped.groupedSpore[A]
          implicit val ww = implicitly[SerDe[Int]].ser(())

          for {
            ev <- EColl.single(n, name = None)
            ecoll <- GenericMap.task(taskID, taskVersion)(
              GenericMap.Input[A, Int, Seq[A]](data1,
                                               Some(ev),
                                               implicitly[SerDe[A]],
                                               implicitly[SerDe[Int]],
                                               implicitly[SerDe[Seq[A]]],
                                               None,
                                               inner,
                                               true,
                                               outName,
                                               taskID,
                                               taskVersion))(resourceRequest)(
              tsc)
          } yield ecoll

    })

}
