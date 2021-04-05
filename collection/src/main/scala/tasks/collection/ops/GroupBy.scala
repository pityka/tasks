package tasks.ecoll.ops

import tasks.ecoll._

import tasks.queue._
import tasks._
import akka.stream.scaladsl.Source
import akka.NotUsed

private[ecoll] object GroupBy {

  def groupBySpore[A, B](
      fun: Spore[A, String],
      aggregate: Spore[Seq[A], B],
      maxParallelJoins: Spore[Unit, Option[Int]],
      numberOfShards: Spore[Unit, Option[Int]]
  ) =
    spore[
      (
          Source[A, NotUsed],
          Option[Nothing],
          ComputationEnvironment,
          SerDe[A],
          SerDe[Nothing]
      ),
      Source[B, NotUsed]
    ] { case (source: Source[A, NotUsed], _: Option[Nothing], ctx, serde, _) =>
      implicit val r = serde.deser(())
      implicit val w = serde.ser(())
      implicit val ce = ctx
      implicit val fmt = EColl.flatJoinFormat[A]
      implicit val sk = new flatjoin.StringKey[A] { def key(t: A) = fun(t) }
      implicit val as = ctx.components.actorsystem
      val parallelismOfJoin =
        math.min(
          resourceAllocated.cpu,
          maxParallelJoins(()).getOrElse(resourceAllocated.cpu)
        )

      source
        .via(
          flatjoin_akka
            .Instance(
              parallelismOfShardComputation = resourceAllocated.cpu,
              numberOfShardsJoinedInParallel = parallelismOfJoin,
              numberOfShards = numberOfShards(()).getOrElse(128)
            )
            .groupByShardsInMemory
        )
        .map(grouped => aggregate(grouped))
    }

  def groupByPresortedSpore[A, B](
      fun: Spore[A, String],
      aggregate: Spore[Seq[A], B]
  ) =
    spore[
      (
          Source[A, NotUsed],
          Option[Nothing],
          ComputationEnvironment,
          SerDe[A],
          SerDe[Nothing]
      ),
      Source[B, NotUsed]
    ] { case (source: Source[A, NotUsed], _, _, _, _) =>
      implicit val sk = new flatjoin.StringKey[A] { def key(t: A) = fun(t) }

      source
        .via(flatjoin_akka.adjacentSpan[A])
        .map(grouped => aggregate(grouped))
    }

  def identitySpore[A] = spore((a: Seq[A]) => a)

}

trait GroupByOps {

  def groupBy[A: SerDe](taskID: String, taskVersion: Int)(
      maxParallelJoins: Spore[Unit, Option[Int]],
      numberOfShards: Spore[Unit, Option[Int]],
      fun: Spore[A, String]
  )(implicit w: SerDe[Seq[A]]): Partial[EColl[A], EColl[Seq[A]]] =
    groupByAndAggregate[A, Seq[A]](taskID, taskVersion)(
      maxParallelJoins,
      numberOfShards,
      fun,
      GroupBy.identitySpore[A]
    )

  def groupByAndAggregate[A: SerDe, B: SerDe](
      taskID: String,
      taskVersion: Int,
      outName: Option[String] = None
  )(
      maxParallelJoins: Spore[Unit, Option[Int]],
      numberOfShards: Spore[Unit, Option[Int]],
      fun: Spore[A, String],
      aggregate: Spore[Seq[A], B]
  ): Partial[EColl[A], EColl[B]] =
    Partial({ case data1 =>
      resourceRequest =>
        tsc =>
          val inner =
            GroupBy.groupBySpore[A, B](
              fun,
              aggregate,
              maxParallelJoins,
              numberOfShards
            )

          GenericMap.task(taskID, taskVersion)(
            GenericMap.Input[A, Nothing, B](
              data1,
              None,
              implicitly[SerDe[A]],
              SerDe.nothing,
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

  def groupByPresorted[A: SerDe](taskID: String, taskVersion: Int)(
      fun: Spore[A, String]
  )(implicit w: SerDe[Seq[A]]): Partial[EColl[A], EColl[Seq[A]]] =
    groupByPresortedAndAggregate[A, Seq[A]](taskID, taskVersion)(
      fun,
      GroupBy.identitySpore[A]
    )

  def groupByPresortedAndAggregate[A: SerDe, B: SerDe](
      taskID: String,
      taskVersion: Int,
      outName: Option[String] = None
  )(
      fun: Spore[A, String],
      aggregate: Spore[Seq[A], B]
  ): Partial[EColl[A], EColl[B]] =
    Partial({ case data1 =>
      resourceRequest =>
        tsc =>
          val inner = GroupBy.groupByPresortedSpore[A, B](fun, aggregate)

          GenericMap.task(taskID, taskVersion)(
            GenericMap.Input[A, Nothing, B](
              data1,
              None,
              implicitly[SerDe[A]],
              SerDe.nothing,
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
