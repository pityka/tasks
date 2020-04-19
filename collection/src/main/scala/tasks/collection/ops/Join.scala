package tasks.ecoll.ops

import tasks.ecoll._

import tasks.queue._
import tasks._
import scala.concurrent.Future
import tasks.circesupport._

private[ecoll] object Join {

  case class Input[AA, BB](
      data1: List[EColl[AA]],
      serdaA: SerDe[AA],
      serdaB: SerDe[BB],
      fun: Spore[AA, String],
      transform: Spore[Seq[Seq[Option[AA]]], List[BB]],
      dropRecordIfEmpty: Seq[Int],
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
    implicit def encoder[A, B]: Encoder[Input[A, B]] =
      deriveEncoder[Input[A, B]]
    implicit def decoder[A, B]: Decoder[Input[A, B]] =
      deriveDecoder[Input[A, B]]

  }

  def identity[A] = spore((a: Seq[Seq[A]]) => a.toList)

  def task[A, B](taskId: String, taskVersion: Int) = TaskDefinition(
    spore(() => implicitly[Deserializer[Input[A, B]]]),
    spore(() => implicitly[Serializer[EColl[B]]]),
    spore[Input[A, B], ComputationEnvironment => Future[EColl[B]]] {
      case Input(
          data1,
          serdeA,
          serdeB,
          fun,
          transform,
          dropRecordIfEmpty,
          maxParallelJoins,
          numberOfShards,
          outName,
          taskId,
          _
          ) =>
        implicit ctx =>
          log.info(taskId)
          implicit val r = serdeA.deser(())
          implicit val wa = serdeA.ser(())
          implicit val w = serdeB.ser(())
          implicit val fmt = EColl.flatJoinFormat[A]
          implicit val sk = new flatjoin.StringKey[A] { def key(t: A) = fun(t) }
          implicit val as = ctx.components.actorsystem
          val parallelismOfJoin =
            math.min(
              resourceAllocated.cpu,
              maxParallelJoins.getOrElse(resourceAllocated.cpu)
            )

          val catted = akka.stream.scaladsl
            .Source(data1.zipWithIndex)
            .flatMapConcat(
              x =>
                x._1
                  .sourceFrom(
                    parallelismOfDeserialization = resourceAllocated.cpu
                  )
                  .map(y => x._2 -> y)
            )

          val joinedSource = catted
            .via(
              flatjoin_akka
                .Instance(
                  parallelismOfShardComputation = resourceAllocated.cpu,
                  numberOfShardsJoinedInParallel = parallelismOfJoin,
                  numberOfShards = numberOfShards.getOrElse(128)
                )
                .joinByShards(data1.size)
            )
            .map { group =>
              group.filterNot { joinedRecord =>
                dropRecordIfEmpty.exists(i => joinedRecord(i).isEmpty)
              }
            }
            .mapConcat(group => transform(group))

          EColl.fromSource(joinedSource, outName, resourceAllocated.cpu)(
            w,
            ctx.components
          )

    },
    TaskId(taskId, taskVersion)
  )

}

trait JoinOps {
  def join[A: SerDe](
      taskID: String,
      taskVersion: Int,
      maxParallelJoins: Option[Int],
      numberOfShards: Option[Int],
      outName: Option[String] = None
  )(key: Spore[A, String])(
      implicit sr: SerDe[Seq[Option[A]]]
  ): Partial[List[(EColl[A], Boolean)], EColl[Seq[Option[A]]]] =
    joinThenMap(taskID, taskVersion, maxParallelJoins, numberOfShards, outName)(
      key,
      Join.identity
    )

  def joinThenMap[A: SerDe, B: SerDe](
      taskID: String,
      taskVersion: Int,
      maxParallelJoins: Option[Int],
      numberOfShards: Option[Int],
      outName: Option[String] = None
  )(
      key: Spore[A, String],
      transform: Spore[Seq[Seq[Option[A]]], List[B]]
  ): Partial[List[(EColl[A], Boolean)], EColl[B]] =
    Partial({
      case data1 =>
        resourceRequest =>
          tsc =>
            Join.task(taskID, taskVersion)(
              Join.Input(
                data1.map(_._1),
                implicitly[SerDe[A]],
                implicitly[SerDe[B]],
                key,
                transform,
                data1.zipWithIndex.filter(_._1._2).map(_._2),
                maxParallelJoins,
                numberOfShards,
                outName,
                taskID,
                taskVersion
              )
            )(resourceRequest)(tsc)
    })

}
