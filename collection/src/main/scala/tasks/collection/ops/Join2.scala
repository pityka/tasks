package tasks.ecoll.ops

import tasks.ecoll._

import tasks.queue._
import tasks._
import scala.concurrent.Future
import tasks.circesupport._

private[ecoll] object Join2 {

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
      dropRecordIfAEmpty: Boolean,
      dropRecordIfBEmpty: Boolean,
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
          dropEmptyA,
          dropEmptyB,
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

          val dropRecordIfEmpty = (dropEmptyA, dropEmptyB) match {
            case (true, true)   => List(0, 1)
            case (true, false)  => List(0)
            case (false, true)  => List(1)
            case (false, false) => Nil
          }

          val joinedSource = catted
            .via(
              flatjoin_akka
                .Instance(
                  parallelismOfShardComputation = resourceAllocated.cpu,
                  numberOfShardsJoinedInParallel = parallelismOfJoin,
                  numberOfShards = numberOfShards.getOrElse(128)
                )
                .joinByShards[Either1[A, B]](2, dropRecordIfEmpty)
            )
            .map { group =>
              group
                .map { joinedRecord =>
                  val a = joinedRecord.collectFirst {
                    case Some((Some(a), _)) => a
                  }
                  val b = joinedRecord.collectFirst {
                    case Some((_, (Some(b)))) => b
                  }
                  (a, b)
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

  def identityList[A] = spore((a: A) => List(a))

  def inner[A, B] =
    spore(
      (a: Seq[(Option[A], Option[B])]) =>
        a.toList.map(p => p._1.get -> p._2.get)
    )
  def innerKeepGroups[A, B] =
    spore(
      (a: Seq[(Option[A], Option[B])]) => List(a.map(p => p._1.get -> p._2.get))
    )
  def outer[A, B] = spore((a: Seq[(Option[A], Option[B])]) => a.toList)
  def leftOuter[A, B] =
    spore(
      (a: Seq[(Option[A], Option[B])]) => a.toList.map(p => p._1 -> p._2.get)
    )
  def rightOuter[A, B] =
    spore(
      (a: Seq[(Option[A], Option[B])]) => a.toList.map(p => p._1.get -> p._2)
    )

  def innerTx[A, B, C](tx: Spore[Seq[(A, B)], List[C]]) =
    spore(
      (a: Seq[(Option[A], Option[B])]) => tx(a.map(p => p._1.get -> p._2.get))
    )
  def outerTx[A, B, C](tx: Spore[Seq[(Option[A], Option[B])], List[C]]) =
    spore((a: Seq[(Option[A], Option[B])]) => tx(a.map(p => p._1 -> p._2)))
  def leftOuterTx[A, B, C](tx: Spore[Seq[(Option[A], B)], List[C]]) =
    spore((a: Seq[(Option[A], Option[B])]) => tx(a.map(p => p._1 -> p._2.get)))
  def rightOuterTx[A, B, C](tx: Spore[Seq[(A, Option[B])], List[C]]) =
    spore((a: Seq[(Option[A], Option[B])]) => tx(a.map(p => p._1.get -> p._2)))

}

trait Join2Ops {
  def join2InnerTx[AA: SerDe, A: SerDe, BB: SerDe, B: SerDe, C: SerDe](
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
      postTransform: Spore[Seq[(A, B)], List[C]]
  )(
      implicit sr: SerDe[(Option[A], Option[B])]
  ): Partial[(EColl[AA], EColl[BB]), EColl[C]] =
    Partial({
      case (d1, d2) =>
        resourceRequest =>
          tsc =>
            val partial =
              join2ThenMap(
                taskID,
                taskVersion,
                maxParallelJoins,
                numberOfShards,
                outName
              )(
                preTransformA,
                preTransformB,
                keyA,
                keyB,
                Join2.innerTx[A, B, C](postTransform)
              )
            partial(((d1, true), (d2, true)))(resourceRequest)(tsc)
    })
  def join2OuterTx[AA: SerDe, A: SerDe, BB: SerDe, B: SerDe, C: SerDe](
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
      case (d1, d2) =>
        resourceRequest =>
          tsc =>
            val partial =
              join2ThenMap(
                taskID,
                taskVersion,
                maxParallelJoins,
                numberOfShards,
                outName
              )(
                preTransformA,
                preTransformB,
                keyA,
                keyB,
                Join2.outerTx[A, B, C](postTransform)
              )
            partial(((d1, false), (d2, false)))(resourceRequest)(tsc)
    })
  def join2LeftOuterTx[AA: SerDe, A: SerDe, BB: SerDe, B: SerDe, C: SerDe](
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
      postTransform: Spore[Seq[(Option[A], B)], List[C]]
  )(
      implicit sr: SerDe[(Option[A], Option[B])]
  ): Partial[(EColl[AA], EColl[BB]), EColl[C]] =
    Partial({
      case (d1, d2) =>
        resourceRequest =>
          tsc =>
            val partial =
              join2ThenMap(
                taskID,
                taskVersion,
                maxParallelJoins,
                numberOfShards,
                outName
              )(
                preTransformA,
                preTransformB,
                keyA,
                keyB,
                Join2.leftOuterTx[A, B, C](postTransform)
              )
            partial(((d1, false), (d2, true)))(resourceRequest)(tsc)
    })
  def join2RightOuterTx[AA: SerDe, A: SerDe, BB: SerDe, B: SerDe, C: SerDe](
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
      postTransform: Spore[Seq[(A, Option[B])], List[C]]
  )(
      implicit sr: SerDe[(Option[A], Option[B])]
  ): Partial[(EColl[AA], EColl[BB]), EColl[C]] =
    Partial({
      case (d1, d2) =>
        resourceRequest =>
          tsc =>
            val partial =
              join2ThenMap(
                taskID,
                taskVersion,
                maxParallelJoins,
                numberOfShards,
                outName
              )(
                preTransformA,
                preTransformB,
                keyA,
                keyB,
                Join2.rightOuterTx[A, B, C](postTransform)
              )
            partial(((d1, true), (d2, false)))(resourceRequest)(tsc)
    })

  def join2Inner[A: SerDe, B: SerDe](
      taskID: String,
      taskVersion: Int,
      maxParallelJoins: Option[Int],
      numberOfShards: Option[Int],
      outName: Option[String] = None
  )(keyA: Spore[A, String], keyB: Spore[B, String])(
      implicit sr: SerDe[(A, B)],
      sr2: SerDe[(Option[A], Option[B])]
  ): Partial[(EColl[A], EColl[B]), EColl[(A, B)]] =
    Partial({
      case (d1, d2) =>
        resourceRequest =>
          tsc =>
            val partial =
              join2ThenMap(
                taskID,
                taskVersion,
                maxParallelJoins,
                numberOfShards,
                outName
              )(
                Join2.identityList[A],
                Join2.identityList[B],
                keyA,
                keyB,
                Join2.inner[A, B]
              )
            partial(((d1, true), (d2, true)))(resourceRequest)(tsc)
    })

  def join2Outer[A: SerDe, B: SerDe](
      taskID: String,
      taskVersion: Int,
      maxParallelJoins: Option[Int],
      numberOfShards: Option[Int],
      outName: Option[String] = None
  )(keyA: Spore[A, String], keyB: Spore[B, String])(
      implicit sr: SerDe[(Option[A], Option[B])]
  ): Partial[(EColl[A], EColl[B]), EColl[(Option[A], Option[B])]] =
    Partial({
      case (d1, d2) =>
        resourceRequest =>
          tsc =>
            val partial =
              join2ThenMap(
                taskID,
                taskVersion,
                maxParallelJoins,
                numberOfShards,
                outName
              )(
                Join2.identityList[A],
                Join2.identityList[B],
                keyA,
                keyB,
                Join2.outer[A, B]
              )
            partial(((d1, false), (d2, false)))(resourceRequest)(tsc)
    })

  def join2LeftOuter[A: SerDe, B: SerDe](
      taskID: String,
      taskVersion: Int,
      maxParallelJoins: Option[Int],
      numberOfShards: Option[Int],
      outName: Option[String] = None
  )(keyA: Spore[A, String], keyB: Spore[B, String])(
      implicit sr: SerDe[(Option[A], B)],
      sr2: SerDe[(Option[A], Option[B])]
  ): Partial[(EColl[A], EColl[B]), EColl[(Option[A], B)]] =
    Partial({
      case (d1, d2) =>
        resourceRequest =>
          tsc =>
            val partial =
              join2ThenMap(
                taskID,
                taskVersion,
                maxParallelJoins,
                numberOfShards,
                outName
              )(
                Join2.identityList[A],
                Join2.identityList[B],
                keyA,
                keyB,
                Join2.leftOuter[A, B]
              )
            partial(((d1, false), (d2, true)))(resourceRequest)(tsc)
    })

  def join2RightOuter[A: SerDe, B: SerDe](
      taskID: String,
      taskVersion: Int,
      maxParallelJoins: Option[Int],
      numberOfShards: Option[Int],
      outName: Option[String] = None
  )(keyA: Spore[A, String], keyB: Spore[B, String])(
      implicit sr: SerDe[(A, Option[B])],
      sr2: SerDe[(Option[A], Option[B])]
  ): Partial[(EColl[A], EColl[B]), EColl[(A, Option[B])]] =
    Partial({
      case (d1, d2) =>
        resourceRequest =>
          tsc =>
            val partial =
              join2ThenMap(
                taskID,
                taskVersion,
                maxParallelJoins,
                numberOfShards,
                outName
              )(
                Join2.identityList[A],
                Join2.identityList[B],
                keyA,
                keyB,
                Join2.rightOuter[A, B]
              )
            partial(((d1, true), (d2, false)))(resourceRequest)(tsc)
    })

  def join2ThenMap[AA: SerDe, A: SerDe, BB: SerDe, B: SerDe, C: SerDe](
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
  ): Partial[((EColl[AA], Boolean), (EColl[BB], Boolean)), EColl[C]] =
    Partial({
      case ((data1, data1Drop), (data2, data2Drop)) =>
        resourceRequest =>
          tsc =>
            Join2.task(taskID, taskVersion)(
              Join2.Input[AA, A, BB, B, C](
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
                data1Drop,
                data2Drop,
                maxParallelJoins,
                numberOfShards,
                outName,
                taskID,
                taskVersion
              )
            )(resourceRequest)(tsc)
    })

}
