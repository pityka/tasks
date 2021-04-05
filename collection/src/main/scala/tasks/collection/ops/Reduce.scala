package tasks.ecoll.ops

import tasks.ecoll._

import tasks.queue._
import tasks._
import scala.concurrent.Future
import tasks.circesupport._

private[ecoll] object Reduce {

  case class Input[AA](
      data: EColl[AA],
      serdaA: SerDe[AA],
      range: Option[Range],
      fun: Spore[(AA, AA), AA],
      outName: Option[String],
      taskID: String,
      taskVersion: Int
  )

  object Input {
    import io.circe._
    import io.circe.generic.semiauto._
    implicit def encoder[A]: Encoder[Input[A]] =
      deriveEncoder[Input[A]]
    implicit def decoder[A]: Decoder[Input[A]] =
      deriveDecoder[Input[A]]

  }

  private[Reduce] def subTask[A](taskId: String, taskVersion: Int) =
    TaskDefinition(
      spore(() => implicitly[Deserializer[Input[A]]]),
      spore(() => implicitly[Serializer[EColl[A]]]),
      spore[Input[A], ComputationEnvironment => Future[EColl[A]]] {
        case Input(data, serdeA, mayRange, fun, outName, taskId, _) =>
          implicit ctx =>
            val range = mayRange.get
            log.info(taskId + "-" + range)
            val r = serdeA.deser(())
            val w = serdeA.ser(())
            EColl.fromSource(
              data
                .sourceOfRange(range)(r, ctx.components)
                .reduce((x, y) => fun((x, y))),
              outName.map(_ + "." + range)
            )(w, ctx.components)
      },
      TaskId(taskId, taskVersion)
    )

  def task[A, B](taskId: String, taskVersion: Int) = TaskDefinition(
    spore(() => implicitly[Deserializer[Input[A]]]),
    spore(() => implicitly[Serializer[EColl[A]]]),
    spore[Input[A], ComputationEnvironment => Future[EColl[A]]] {
      case input @ Input(data, serde, _, fun, outName, taskId, taskVersion) =>
        implicit ctx =>
          releaseResources
          implicit val r = serde.deser(())
          implicit val w = serde.ser(())
          for {
            ranges <- data.ranges(8)
            subResults <- Future
              .sequence(ranges map { range =>
                Reduce.subTask(taskId + "-sub", taskVersion)(
                  input.copy(range = Some(range))
                )(
                  ResourceRequest(
                    resourceAllocated.cpu,
                    resourceAllocated.memory,
                    resourceAllocated.scratch
                  )
                )
              })
            result <- {
              val reducedSource = subResults
                .map(_.sourceFrom())
                .reduce(_ ++ _)
                .reduce((x, y) => fun((x, y)))
              EColl.fromSource(reducedSource, name = outName, 1)
            }
            _ <- Future.traverse(subResults)(_.delete)
          } yield result

    },
    TaskId(taskId, taskVersion)
  )
}

trait ReduceOps {
  def reduce[A: SerDe](
      taskID: String,
      taskVersion: Int,
      outName: Option[String] = None
  )(
      fun: Spore[(A, A), A]
  ): Partial[EColl[A], EColl[A]] =
    Partial(data =>
      resourceRequest =>
        tsc =>
          Reduce.task(taskID, taskVersion)(
            Reduce.Input(
              data,
              implicitly[SerDe[A]],
              None,
              fun,
              outName,
              taskID,
              taskVersion
            )
          )(resourceRequest)(tsc)
    )
}
