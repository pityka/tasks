package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import org.ekrich.config.ConfigFactory

import scala.concurrent.duration._

import tasks.jsonitersupport._
import tasks.queue.QueueImpl
import tasks.util.Transaction

object TaskReportRetryTest extends TestHelpers {

  val echo = tasks.Task[Input, Int]("taskReportRetry", 1) { in => _ =>
    IO.pure(in.i)
  }

  val config = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      tasks.cache.enabled = false
      hosts.numCPU = 1
      tasks.disableRemoting = false
      tasks.addShutdownHook = false
      tasks.askInterval = 250 millis
      """
    )
  }

  /* Fails the first `failFirst` state updates which move a task out of
   * scheduledTasks and store a result for its proxy. That pair of changes only
   * happens in taskSuccess, so this fails the launcher's report of a completed
   * task without touching enqueueing, scheduling, polling or heartbeating.
   */
  final class FailsFirstCompletionReports(
      underlying: Transaction[QueueImpl.State],
      failures: Ref[IO, Int],
      failFirst: Int
  ) extends Transaction[QueueImpl.State] {

    def get: IO[QueueImpl.State] = underlying.get

    def flatModify[B](
        update: QueueImpl.State => (QueueImpl.State, IO[B])
    ): IO[B] =
      underlying.get.flatMap { before =>
        val after = update(before)._1
        val reportsACompletedTask =
          after.scheduledTasks.size < before.scheduledTasks.size &&
            after.completedResults.size > before.completedResults.size

        failures.get.flatMap { alreadyFailed =>
          if (reportsACompletedTask && alreadyFailed < failFirst)
            failures.update(_ + 1) *> IO.raiseError[B](
              new RuntimeException("transient queue state failure")
            )
          else underlying.flatModify(update)
        }
      }
  }

  def run: IO[(Int, Int)] =
    for {
      failures <- Ref.of[IO, Int](0)
      stateRef <- Ref.of[IO, QueueImpl.State](QueueImpl.State.empty)
      transaction = new FailsFirstCompletionReports(
        Transaction.fromRef(stateRef),
        failures,
        failFirst = 2
      )
      result <- withTaskSystem(
        config = Some(config),
        s3Client = Resource.pure(None),
        elasticSupport = Resource.pure(None),
        externalQueueState = Resource.pure(Some(transaction))
      ) { implicit ts =>
        echo(Input(42))(tasks.ResourceRequest(1, 500))
      }
      observed <- failures.get
    } yield (result.toOption.get, observed)

}

class TaskReportRetryTestSuite extends FunSuite with Matchers {

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Error))
    .replace()

  test(
    "a completed task reaches its caller when reporting it to the queue transiently fails"
  ) {
    val (result, failures) = TaskReportRetryTest.run
      .unsafeRunTimed(60.seconds)
      .getOrElse(
        fail(
          "the task never completed: its result was dropped after the report to the queue failed"
        )
      )

    result shouldBe 42

    withClue("the injected failure path was never exercised: ") {
      failures shouldBe 2
    }
  }

}
