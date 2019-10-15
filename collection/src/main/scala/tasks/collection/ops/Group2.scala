package tasks.ecoll.ops

import tasks.ecoll._

import tasks.queue._
import tasks._
import scala.concurrent.Future
import tasks.circesupport._

private[ecoll] object Group2 {

  case class Input[AA, A, BB, B, CC](
      data1: EColl[AA],
      data2: EColl[BB],
      serdaA: SerDe[AA],
      serdaB: SerDe[BB],
      serdeAB: SerDe[(Option[A], Option[B])],
      serdeC: SerDe[CC],
      funA: Spore[A, String],
      funB: Spore[B, String],
      preTransformA: Spore[AA, List[A]],
      preTransformB: Spore[BB, List[B]],
      postTransform: Spore[Seq[(Option[A], Option[B])], List[CC]],
      maxParallelJoins: Option[Int],
      numberOfShards: Option[Int],
      outName: Option[String],
      taskID: String,
      taskVersion: Int
  )

  object Input {
    import io.circe._
    import io.circe.generic.semiauto._
    import io.circe.generic.auto._
    implicit def encoder[AA, A, BB, B, CC]: Encoder[Input[AA, A, BB, B, CC]] =
      deriveEncoder[Input[AA, A, BB, B, CC]]
    implicit def decoder[AA, A, BB, B, CC]: Decoder[Input[AA, A, BB, B, CC]] =
      deriveDecoder[Input[AA, A, BB, B, CC]]

  }

  def right[A, B](b: B) = (Option.empty[A], Option(b))
  def left[A, B](a: A) = (Option(a), Option.empty[B])
  type Either1[A, B] = (Option[A], Option[B])

  def task[AA, A, BB, B, C](taskId: String, taskVersion: Int) = TaskDefinition(
    spore(() => implicitly[Deserializer[Input[AA, A, BB, B, C]]]),
    spore(() => implicitly[Serializer[EColl[C]]]),
    spore[Input[AA, A, BB, B, C], ComputationEnvironment => Future[EColl[C]]] {
      case Input(
          data1,
          data2,
          serdeA,
          serdeB,
          serdeAB,
          serdeC,
          funA,
          funB,
          preTransformA,
          preTransformB,
          transform,
          maxParallelJoins,
          numberOfShards,
          outName,
          taskId,
          _
          ) =>
        implicit ctx =>
          log.info(taskId)
          implicit val raa = serdeA.deser(())
          implicit val rbb = serdeB.deser(())
          implicit val rab = serdeAB.deser(())
          implicit val wab = serdeAB.ser(())
          implicit val wc = serdeC.ser(())
          implicit val fmt = EColl.flatJoinFormat[(Option[A], Option[B])]
          implicit val mat = ctx.components.actorMaterializer
          val parallelismOfJoin =
            math.min(
              resourceAllocated.cpu,
              maxParallelJoins.getOrElse(resourceAllocated.cpu)
            )

          val catted = data1
            .sourceFrom(parallelismOfDeserialization = resourceAllocated.cpu)
            .mapConcat(a => preTransformA(a))
            .map(y => 0 -> left(y)) ++
            data2
              .sourceFrom(parallelismOfDeserialization = resourceAllocated.cpu)
              .mapConcat(b => preTransformB(b))
              .map(y => 1 -> right(y))

          implicit val skEither = new flatjoin.StringKey[Either1[A, B]] {
            def key(t: Either1[A, B]) = t match {
              case (Some(t), None) => funA(t)
              case (None, Some(t)) => funB(t)
              case _               => throw new RuntimeException("should not happen")
            }
          }

          val joinedSource = catted
            .via(
              flatjoin_akka
                .Instance(
                  parallelismOfShardComputation = resourceAllocated.cpu,
                  numberOfShardsJoinedInParallel = parallelismOfJoin,
                  numberOfShards = numberOfShards.getOrElse(128)
                )
                .groupByShardsInMemory[Either1[A, B]](2)
            )
            .map { group =>
              group
                .map(_._2)
                .filterNot { joinedRecord =>
                  joinedRecord._1.isEmpty && joinedRecord._2.isEmpty
                }
            }
            .mapConcat { group =>
              transform(group)
            }

          EColl.fromSource(joinedSource, outName, resourceAllocated.cpu)(
            wc,
            ctx.components
          )

    },
    TaskId(taskId, taskVersion)
  )

}

trait Group2Ops {

  def group2[AA: SerDe, A: SerDe, BB: SerDe, B: SerDe, C: SerDe](
      taskID: String,
      taskVersion: Int,
      maxParallelJoins: Option[Int],
      numberOfShards: Option[Int],
      outName: Option[String] = None
  )(
      preTransformA: Spore[AA, List[A]],
      preTransformB: Spore[BB, List[B]],
      keyA: Spore[A, String],
      keyB: Spore[B, String],
      postTransform: Spore[Seq[(Option[A], Option[B])], List[C]]
  )(
      implicit sr: SerDe[(Option[A], Option[B])]
  ): Partial[(EColl[AA], EColl[BB]), EColl[C]] =
    Partial({
      case (data1, data2) =>
        resourceRequest =>
          tsc =>
            Group2.task(taskID, taskVersion)(
              Group2.Input[AA, A, BB, B, C](
                data1,
                data2,
                implicitly[SerDe[AA]],
                implicitly[SerDe[BB]],
                implicitly[SerDe[(Option[A], Option[B])]],
                implicitly[SerDe[C]],
                keyA,
                keyB,
                preTransformA,
                preTransformB,
                postTransform,
                maxParallelJoins,
                numberOfShards,
                outName,
                taskID,
                taskVersion
              )
            )(resourceRequest)(tsc)
    })

}
