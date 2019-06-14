package tasks.ecoll.ops

import tasks.ecoll._

import tasks.queue._
import tasks._
import scala.concurrent.Future
import tasks.circesupport._

private[ecoll] object SimpleMap {

  case class Input[AA, BB](
      data: EColl[AA],
      serdaA: SerDe[AA],
      serdeB: SerDe[BB],
      range: Option[Range],
      fun: Spore[AA, BB],
      outName: Option[String],
      taskID: String,
      taskVersion: Int
  )
  object Input {
    import io.circe._
    import io.circe.generic.semiauto._
    implicit def encoder[A, B]: Encoder[Input[A, B]] =
      deriveEncoder[Input[A, B]]
    implicit def decoder[A, B]: Decoder[Input[A, B]] =
      deriveDecoder[Input[A, B]]

  }

  private[SimpleMap] def subTask[A, B](taskId: String, taskVersion: Int) =
    TaskDefinition(
      spore(() => implicitly[Deserializer[Input[A, B]]]),
      spore(() => implicitly[Serializer[EColl[B]]]),
      spore[Input[A, B], ComputationEnvironment => Future[EColl[B]]] {
        case Input(data, serdeA, serdeB, mayRange, fun, outName, taskId, _) =>
          implicit ctx =>
            val range = mayRange.get
            log.info(taskId + "-" + range)
            val r = serdeA.deser(())
            val w = serdeB.ser(())
            EColl.fromSource(
              data
                .sourceOfRange(range)(r, ctx.components)
                .map(x => fun(x)),
              outName.map(_ + "." + range)
            )(w, ctx.components)
      },
      TaskId(taskId, taskVersion)
    )

  def task[A, B](taskId: String, taskVersion: Int) = TaskDefinition(
    spore(() => implicitly[Deserializer[Input[A, B]]]),
    spore(() => implicitly[Serializer[EColl[B]]]),
    spore[Input[A, B], ComputationEnvironment => Future[EColl[B]]] {
      case input @ Input(data, _, _, _, _, _, taskId, taskVersion) =>
        implicit ctx =>
          releaseResources
          for {
            ranges <- data.ranges(8)
            subResults <- Future
              .sequence(ranges map { range =>
                SimpleMap.subTask(taskId + "-sub", taskVersion)(
                  input.copy(range = Some(range))
                )(
                  ResourceRequest(
                    resourceAllocated.cpu,
                    resourceAllocated.memory,
                    resourceAllocated.scratch
                  )
                )
              })
            result <- EColl.concatenate(subResults)
            _ <- Future.traverse(subResults)(_.delete)
          } yield result

    },
    TaskId(taskId, taskVersion)
  )
}

trait SimpleMapOps {
  def map[A: SerDe, B: SerDe](
      taskID: String,
      taskVersion: Int,
      outName: Option[String] = None
  )(fun: Spore[A, B]): Partial[EColl[A], EColl[B]] =
    Partial(
      data =>
        resourceRequest =>
          tsc =>
            SimpleMap.task(taskID, taskVersion)(
              SimpleMap.Input(
                data,
                implicitly[SerDe[A]],
                implicitly[SerDe[B]],
                None,
                fun,
                outName,
                taskID,
                taskVersion
              )
            )(resourceRequest)(tsc)
    )
}
