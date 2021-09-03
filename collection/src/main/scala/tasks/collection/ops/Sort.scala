package tasks.ecoll.ops

import tasks.ecoll._

import tasks.queue._
import tasks._
import scala.concurrent.Future
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
private[ecoll] object Sort {

  case class Input[AA](
      unsorted: EColl[AA],
      serdaA: SerDe[AA],
      range: Option[Range],
      fun: Spore[AA, String],
      outName: Option[String],
      taskID: String,
      taskVersion: Int
  )

  object Input {
    implicit def codec[A]: JsonValueCodec[Input[A]] =
      JsonCodecMaker.make

  }

  private[Sort] def subTask[A](taskId: String, taskVersion: Int) =
    TaskDefinition(
      spore(() => implicitly[Deserializer[Input[A]]]),
      spore(() => implicitly[Serializer[EColl[A]]]),
      spore[Input[A], ComputationEnvironment => Future[EColl[A]]] {
        case Input(unsorted, serdeA, mayRange, fun, outName, taskId, _) =>
          implicit ctx =>
            val range = mayRange.get
            log.info(taskId + "-" + range)
            implicit val as = ctx.components.actorsystem
            implicit val r = serdeA.deser(())
            implicit val w = serdeA.ser(())

            implicit val fmt = EColl.flatJoinFormat[A]
            implicit val sk = new flatjoin.StringKey[A] {
              def key(t: A) = fun(t)
            }
            val sortedSource = unsorted
              .sourceOfRange(range)(r, ctx.components)
              .via(flatjoin_akka.Instance().sort)
            EColl.fromSource(sortedSource, outName.map(_ + "." + range))(
              w,
              ctx.components
            )

      },
      TaskId(taskId, taskVersion)
    )

  def task[A](taskId: String, taskVersion: Int) = TaskDefinition(
    spore(() => implicitly[Deserializer[Input[A]]]),
    spore(() => implicitly[Serializer[EColl[A]]]),
    spore[Input[A], ComputationEnvironment => Future[EColl[A]]] {
      case input @ Input(
            unsorted,
            serdeA,
            _,
            fun,
            outName,
            taskId,
            taskVersion
          ) =>
        implicit ctx =>
          releaseResources
          for {
            ranges <- unsorted.ranges(8)
            sortedPartitions <- scala.concurrent.Future
              .traverse(ranges) { range =>
                subTask(taskId + "-sub", taskVersion)(
                  input.copy(range = Some(range))
                )(
                  ResourceRequest(
                    resourceAllocated.cpu,
                    resourceAllocated.memory,
                    resourceAllocated.scratch
                  )
                )
              }
            result <-
              if (sortedPartitions.size == 1)
                scala.concurrent.Future.successful(sortedPartitions.head)
              else {
                implicit val r = serdeA.deser(())
                implicit val w = serdeA.ser(())
                implicit val sk = new flatjoin.StringKey[A] {
                  def key(t: A) = fun(t)
                }

                val fileReader = (f: SharedFile) =>
                  EColl
                    .decodeFileForFlatJoin(r, sk, resourceAllocated.cpu)(f)(
                      ctx.components.executionContext,
                      ctx.components
                    )

                val source = {
                  implicit val ordering: Ordering[(String, A)] =
                    Ordering.by(_._1)
                  sortedPartitions.toList
                    .map(_.data)
                    .map(fileReader)
                    .reduce((s1, s2) => s1.mergeSorted(s2))
                    .map { case (_, t) =>
                      t
                    }
                }

                for {
                  merged <- {

                    EColl.fromSource(source, outName, resourceAllocated.cpu)(
                      w,
                      ctx.components
                    )
                  }
                  _ <- Future.traverse(sortedPartitions)(_.data.delete)
                } yield merged
              }
          } yield result
    },
    TaskId(taskId, taskVersion)
  )

}

trait SortOps {
  def sortBy[A: SerDe](
      taskID: String,
      taskVersion: Int,
      outName: Option[String] = None
  )(fun: Spore[A, String]): Partial[EColl[A], EColl[A]] =
    Partial({ case unsorted =>
      resourceRequest =>
        tsc =>
          Sort.task(taskID, taskVersion)(
            Sort
              .Input(
                unsorted,
                implicitly[SerDe[A]],
                None,
                fun,
                outName,
                taskID,
                taskVersion
              )
          )(resourceRequest)(tsc)
    })

}
