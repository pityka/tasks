package tasks.ecoll.ops

import tasks.ecoll._

import tasks.queue._
import tasks._
import akka.stream.scaladsl.Source
import akka.NotUsed

private[ecoll] object Scan {

  def scanSpore[A, B](fun: Spore[(B, A), B]) =
    spore[
      (
          Source[A, NotUsed],
          Option[B],
          ComputationEnvironment,
          SerDe[A],
          SerDe[B]
      ),
      Source[B, NotUsed]
    ] {
      case (source: Source[A, NotUsed], const: Option[B], _, _, _) =>
        source.scan(const.get)((b, a) => fun.apply((b, a)))
    }

}

trait ScanOps {

  def scan[A: SerDe, B: SerDe](
      taskID: String,
      taskVersion: Int,
      outName: Option[String] = None
  )(fun: Spore[(B, A), B]): Partial[(EColl[A], EColl[B]), EColl[B]] =
    Partial({
      case (data1, data2) =>
        resourceRequest =>
          tsc =>
            val inner = Scan.scanSpore[A, B](fun)

            GenericMap.task(taskID, taskVersion)(
              GenericMap.Input[A, B, B](
                data1,
                Some(data2),
                implicitly[SerDe[A]],
                implicitly[SerDe[B]],
                implicitly[SerDe[B]],
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
