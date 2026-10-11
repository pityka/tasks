package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.ExitCode
import cats.effect.kernel.Deferred
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global

import org.ekrich.config.ConfigFactory
import scala.concurrent.duration._

import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros._

import tasks.jsonitersupport._
import tasks.queue.QueueImpl
import tasks.util.Transaction

object CacheHitExternalQueueNoWriteTest {

  case class In(i: Int)
  object In {
    implicit val codec: JsonValueCodec[In] = JsonCodecMaker.make
  }
  case class Out(i: Int)
  object Out {
    implicit val codec: JsonValueCodec[Out] = JsonCodecMaker.make
  }

  val increment: TaskDefinition[In, Out] =
    Task[In, Out]("cache-hit-external-queue-no-write", 1) { in => _ =>
      IO.pure(Out(in.i + 1))
    }

  final class CountingTransaction(
      underlying: Transaction[QueueImpl.State],
      completedResultWrites: Ref[IO, Int]
  ) extends Transaction[QueueImpl.State] {
    def get: IO[QueueImpl.State] = underlying.get
    def flatModify[B](
        f: QueueImpl.State => (QueueImpl.State, IO[B])
    ): IO[B] =
      underlying.flatModify { state =>
        val (newState, io) = f(state)
        val count =
          if (newState.completedResults != state.completedResults)
            completedResultWrites.update(_ + 1)
          else IO.unit
        (newState, count *> io)
      }
  }
}

class CacheHitExternalQueueNoWriteTestSuite extends FunSuite with Matchers {

  import CacheHitExternalQueueNoWriteTest._

  private def config(storage: String) =
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=$storage
      tasks.cache.enabled = true
      hosts.numCPU = 2
      tasks.disableRemoting = false
      tasks.addShutdownHook = false
      """
    )

  private def taskSystem(
      storage: String,
      external: Option[Transaction[QueueImpl.State]]
  ): Resource[IO, TaskSystemComponents] =
    Resource
      .eval(Deferred[IO, ExitCode])
      .flatMap { exitCode =>
        tasks.defaultTaskSystem(
          config = Some(config(storage)),
          s3Client = Resource.pure(None),
          elasticSupport = Resource.pure(None),
          externalQueueState = Resource.pure(external),
          exitCode = exitCode
        )
      }
      .map(_._1)

  private def countingTransaction: IO[(Transaction[QueueImpl.State], IO[Int])] =
    for {
      stateRef <- Ref.of[IO, QueueImpl.State](QueueImpl.State.empty)
      writes <- Ref.of[IO, Int](0)
    } yield (
      new CountingTransaction(Transaction.fromRef(stateRef), writes),
      writes.get
    )

  test(
    "cache-hit replay against an external queue state performs no completed-result writes"
  ) {
    val replayCount = 20

    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    val storage = tmp.getAbsolutePath

    val program = for {
      populateSetup <- countingTransaction
      (populateTransaction, populateWrites) = populateSetup
      first <- taskSystem(storage, Some(populateTransaction)).use {
        implicit ts =>
          increment(In(1))(ResourceRequest(1, 500))
      }
      populateCount <- populateWrites

      replaySetup <- countingTransaction
      (replayTransaction, replayWrites) = replaySetup
      replayed <- taskSystem(storage, Some(replayTransaction)).use {
        implicit ts =>
          (1 to replayCount).toList.foldLeft(IO.pure(List.empty[Out])) {
            (acc, _) =>
              acc.flatMap(soFar =>
                increment(In(1))(ResourceRequest(1, 500)).map(out =>
                  soFar :+ out
                )
              )
          }
      }
      replayCountWrites <- replayWrites
    } yield (first, populateCount, replayed, replayCountWrites)

    val (first, populateCount, replayed, replayCountWrites) = program
      .unsafeRunTimed(120.seconds)
      .getOrElse(throw new RuntimeException("timeout"))

    first shouldBe Out(2)
    populateCount should be > 0
    replayed shouldBe List.fill(replayCount)(Out(2))
    replayCountWrites shouldBe 0
  }
}
