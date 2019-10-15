package tasks.ecoll.ops

import tasks.ecoll._

import tasks.queue._
import tasks._
import scala.concurrent.Future
import tasks.circesupport._
import akka.stream.scaladsl.Source
import akka.NotUsed

private[ecoll] object GenericMap {

  case class Input[AA, BB, CC](
      data1: EColl[AA],
      data2: Option[EColl[BB]],
      serdaA: SerDe[AA],
      serdeB: SerDe[BB],
      serdeC: SerDe[CC],
      range: Option[Range],
      fun: Spore[
        (
            Source[AA, akka.NotUsed],
            Option[BB],
            ComputationEnvironment,
            SerDe[AA],
            SerDe[BB]
        ),
        Source[CC, akka.NotUsed]
      ],
      parallelize: Boolean,
      outName: Option[String],
      taskID: String,
      taskVersion: Int
  )

  object Input {
    import io.circe._
    import io.circe.generic.semiauto._
    import io.circe.generic.auto._
    implicit def encoder[A, B, C]: Encoder[Input[A, B, C]] =
      deriveEncoder[Input[A, B, C]]
    implicit def decoder[A, B, C]: Decoder[Input[A, B, C]] =
      deriveDecoder[Input[A, B, C]]

  }

  private[GenericMap] def subTask[A, B, C](taskId: String, taskVersion: Int) =
    TaskDefinition(
      spore(() => implicitly[Deserializer[Input[A, B, C]]]),
      spore(() => implicitly[Serializer[EColl[C]]]),
      spore[Input[A, B, C], ComputationEnvironment => Future[EColl[C]]] {
        case Input(
            data1,
            data2,
            serdeA,
            serdeB,
            serdeC,
            mayRange,
            fun,
            _,
            outName,
            taskId,
            _
            ) =>
          implicit ctx =>
            val range = mayRange.get
            log.info(taskId + "-" + range)
            val r = serdeA.deser(())
            val w = serdeC.ser(())
            val rb = serdeB.deser(())
            val d2 = data2 match {
              case None => Future.successful(None)
              case Some(ecoll) =>
                ecoll.head(rb, ctx.toTaskSystemComponents)
            }
            for {
              d2 <- d2
              source = data1.sourceOfRange(range)(r, ctx.components)
              mappedSource = fun((source, d2, ctx, serdeA, serdeB))
              part <- EColl.fromSource(
                mappedSource,
                outName.map(_ + "." + range)
              )(w, ctx.components)
            } yield part

      },
      TaskId(taskId, taskVersion)
    )

  def task[A, B, C](taskId: String, taskVersion: Int) = TaskDefinition(
    spore(() => implicitly[Deserializer[Input[A, B, C]]]),
    spore(() => implicitly[Serializer[EColl[C]]]),
    spore[Input[A, B, C], ComputationEnvironment => Future[EColl[C]]] {
      case input @ Input(
            data1,
            data2,
            serdeA,
            serdeB,
            serdeC,
            _,
            fun,
            parallel,
            outName,
            taskId,
            taskVersion
          ) =>
        implicit ctx =>
          if (parallel) {
            releaseResources
            for {
              ranges <- data1.ranges(8)
              subResults <- Future
                .sequence(ranges map { range =>
                  GenericMap.subTask(taskId + "-sub", taskVersion)(
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
          } else {
            log.info(taskId)
            val r = serdeA.deser(())
            val w = serdeC.ser(())
            val rb = serdeB.deser(())
            val d2 = data2 match {
              case None => Future.successful(None)
              case Some(ecoll) =>
                ecoll.head(rb, ctx.toTaskSystemComponents)
            }
            for {
              d2 <- d2
              source = data1.sourceFrom(
                parallelismOfDeserialization = resourceAllocated.cpu
              )(r, ctx.components)
              mappedSource = fun((source, d2, ctx, serdeA, serdeB))
              part <- EColl
                .fromSource(mappedSource, outName)(w, ctx.components)
            } yield part
          }
    },
    TaskId(taskId, taskVersion)
  )

  def adaptedDefined[AA, BB, CC](
      sp: Spore[
        (
            Source[AA, akka.NotUsed],
            BB,
            ComputationEnvironment,
            SerDe[AA],
            SerDe[BB]
        ),
        Source[CC, akka.NotUsed]
      ]
  ): Spore[
    (
        Source[AA, akka.NotUsed],
        Option[BB],
        ComputationEnvironment,
        SerDe[AA],
        SerDe[BB]
    ),
    Source[CC, akka.NotUsed]
  ] =
    spore {
      case (a, b, ce, sA, sB) => sp((a, b.get, ce, sA, sB))
    }

  def adaptedEmpty[AA, BB, CC](
      sp: Spore[
        (Source[AA, akka.NotUsed], ComputationEnvironment, SerDe[AA], SerDe[BB]),
        Source[CC, akka.NotUsed]
      ]
  ): Spore[
    (
        Source[AA, akka.NotUsed],
        Option[BB],
        ComputationEnvironment,
        SerDe[AA],
        SerDe[BB]
    ),
    Source[CC, akka.NotUsed]
  ] =
    spore {
      case (a, _, ce, sA, sB) => sp((a, ce, sA, sB))
    }

}

trait GenericMapOps {
  def mapWith[A: SerDe, B: SerDe, C: SerDe](
      taskID: String,
      taskVersion: Int,
      parallelize: Boolean
  )(
      fun: Spore[
        (Source[A, NotUsed], B, ComputationEnvironment, SerDe[A], SerDe[B]),
        Source[C, NotUsed]
      ]
  ): Partial[(EColl[A], B), EColl[C]] =
    Partial({
      case (data1, data2) =>
        resource =>
          implicit tsc =>
            implicit val ec = tsc.executionContext
            implicit val w = implicitly[SerDe[B]].ser(())
            val p = genericMap(taskID, taskVersion, parallelize)(
              GenericMap.adaptedDefined(fun)
            )

            for {
              b <- EColl.single(data2, None)
              r <- p((data1, Some(b)))(resource)(tsc)
            } yield r

    })
  def mapWithFirst[A: SerDe, B: SerDe, C: SerDe](
      taskID: String,
      taskVersion: Int,
      parallelize: Boolean
  )(
      fun: Spore[
        (Source[A, NotUsed], B, ComputationEnvironment, SerDe[A], SerDe[B]),
        Source[C, NotUsed]
      ]
  ): Partial[(EColl[A], EColl[B]), EColl[C]] =
    Partial({
      case (data1, data2) =>
        resource =>
          tsc =>
            val p = genericMap(taskID, taskVersion, parallelize)(
              GenericMap.adaptedDefined(fun)
            )
            p((data1, Some(data2)))(resource)(tsc)
    })

  def mapWithContext[A: SerDe, B: SerDe, C: SerDe](
      taskID: String,
      taskVersion: Int,
      parallelize: Boolean
  )(
      fun: Spore[
        (Source[A, NotUsed], ComputationEnvironment, SerDe[A], SerDe[B]),
        Source[C, NotUsed]
      ]
  ): Partial[(EColl[A], EColl[B]), EColl[C]] =
    Partial({
      case (data1, data2) =>
        resource =>
          tsc =>
            val p = genericMap(taskID, taskVersion, parallelize)(
              GenericMap.adaptedEmpty(fun)
            )
            p((data1, Some(data2)))(resource)(tsc)
    })

  def genericMap[A: SerDe, B: SerDe, C: SerDe](
      taskID: String,
      taskVersion: Int,
      parallelize: Boolean,
      outName: Option[String] = None
  )(
      fun: Spore[
        (
            Source[A, NotUsed],
            Option[B],
            ComputationEnvironment,
            SerDe[A],
            SerDe[B]
        ),
        Source[C, NotUsed]
      ]
  ): Partial[(EColl[A], Option[EColl[B]]), EColl[C]] =
    Partial({
      case (data1, data2) =>
        resourceRequest =>
          tsc =>
            GenericMap.task(taskID, taskVersion)(
              GenericMap.Input(
                data1,
                data2,
                implicitly[SerDe[A]],
                implicitly[SerDe[B]],
                implicitly[SerDe[C]],
                None,
                fun,
                parallelize,
                outName,
                taskID,
                taskVersion
              )
            )(resourceRequest)(tsc)
    })

}
