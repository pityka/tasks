package tasks.ecoll.ops

import tasks.ecoll._

import tasks.queue._
import tasks._
import scala.concurrent.Future
import tasks.jsonitersupport._
import akka.stream.scaladsl.Source
import akka.NotUsed
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

private[ecoll] object GenericMap2 {

  case class Input[AA, BB, CC](
      data1: EColl[AA],
      data2: EColl[BB],
      serdaA: SerDe[AA],
      serdeB: SerDe[BB],
      serdeC: SerDe[CC],
      range: Option[Range],
      fun: Spore[
        (
            Source[AA, akka.NotUsed],
            Source[BB, akka.NotUsed],
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
    implicit def codec[A, B, C]: JsonValueCodec[Input[A, B, C]] =
      JsonCodecMaker.make

  }

  private[GenericMap2] def subTask[A, B, C](taskId: String, taskVersion: Int) =
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
            val ra = serdeA.deser(())
            val rb = serdeB.deser(())
            val w = serdeC.ser(())
            val d2 = data2.sourceFrom(
              parallelismOfDeserialization = resourceAllocated.cpu
            )(rb, ctx.components)
            val d1 = data1.sourceOfRange(range)(ra, ctx.components)
            val mappedSource = fun((d1, d2, ctx, serdeA, serdeB))
            for {
              part <- EColl.fromSource(
                mappedSource,
                outName.map(_ + "." + range)
              )(w, ctx.components)
            } yield part

      },
      TaskId(taskId, taskVersion)
    )

  def task[A, B, C](
      taskId: String,
      taskVersion: Int
  ) = TaskDefinition(
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
                  GenericMap2.subTask(taskId + "-sub", taskVersion)(
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
            val ra = serdeA.deser(())
            val rb = serdeB.deser(())
            val w = serdeC.ser(())
            val d2 = data2.sourceFrom(
              parallelismOfDeserialization = resourceAllocated.cpu
            )(rb, ctx.components)
            val d1 = data1.sourceFrom(
              parallelismOfDeserialization = resourceAllocated.cpu
            )(ra, ctx.components)
            val mappedSource = fun((d1, d2, ctx, serdeA, serdeB))
            for {
              part <- EColl
                .fromSource(mappedSource, outName)(w, ctx.components)
            } yield part
          }
    },
    TaskId(taskId, taskVersion)
  )

}

trait GenericMap2Ops {
  def genericMap2[A: SerDe, B: SerDe, C: SerDe](
      taskID: String,
      taskVersion: Int,
      parallelize: Boolean,
      outName: Option[String] = None
  )(
      fun: Spore[
        (
            Source[A, NotUsed],
            Source[B, NotUsed],
            ComputationEnvironment,
            SerDe[A],
            SerDe[B]
        ),
        Source[C, NotUsed]
      ]
  ): Partial[(EColl[A], EColl[B]), EColl[C]] =
    Partial({ case (data1, data2) =>
      resourceRequest =>
        tsc =>
          GenericMap2.task(taskID, taskVersion)(
            GenericMap2.Input(
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
