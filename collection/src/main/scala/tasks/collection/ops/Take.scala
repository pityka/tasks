package tasks.ecoll.ops

import tasks.ecoll._

import tasks.queue._
import tasks._
import akka.stream.scaladsl.Source
import akka.NotUsed

private[ecoll] object Take {

  def takeSpore[A] =
    spore[(Source[A, NotUsed],
           Option[Long],
           ComputationEnvironment,
           SerDe[A],
           SerDe[Long]),
          Source[A, NotUsed]] {
      case (source: Source[A, NotUsed], n, _, _, _) =>
        source.take(n.get)
    }

}

trait TakeOps {

  def take[A: SerDe](taskID: String,
                     taskVersion: Int,
                     outName: Option[String] = None)(
      implicit r: SerDe[Long]): Partial[(EColl[A], Long), EColl[A]] =
    Partial({
      case (data1, n: Long) =>
        resourceRequest => implicit tsc =>
          implicit val ec = tsc.executionContext
          val inner = Take.takeSpore[A]
          implicit val ww = implicitly[SerDe[Long]].ser(())

          for {
            ev <- EColl.single(n, None)
            ecoll <- GenericMap.task(taskID, taskVersion)(
              GenericMap.Input[A, Long, A](data1,
                                           Some(ev),
                                           implicitly[SerDe[A]],
                                           implicitly[SerDe[Long]],
                                           implicitly[SerDe[A]],
                                           None,
                                           inner,
                                           false,
                                           outName,
                                           taskID,
                                           taskVersion))(resourceRequest)(tsc)
          } yield ecoll

    })

}
