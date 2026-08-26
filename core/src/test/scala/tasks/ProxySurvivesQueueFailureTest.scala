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

object ProxySurvivesQueueFailureTest extends TestHelpers {

  val slowTask = tasks.Task[Input, Int]("proxySurvivesQueueFailure", 1) {
    in => _ => IO.sleep(1.second).as(in.i)
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

  /* Fails a state update only when the task is already in the queue, no
   * result is waiting to be picked up, and the update would leave the state
   * unchanged. A poll that finds no result yet is exactly that shape, so this
   * fails the call the proxy makes while it waits without ever touching
   * enqueueing, scheduling or result delivery, all of which change the state.
   */
  final class FailsIdlePolls(
      underlying: Transaction[QueueImpl.State],
      failures: Ref[IO, Int]
  ) extends Transaction[QueueImpl.State] {

    def get: IO[QueueImpl.State] = underlying.get

    def flatModify[B](
        update: QueueImpl.State => (QueueImpl.State, IO[B])
    ): IO[B] =
      underlying.get.flatMap { before =>
        val taskIsInTheQueue =
          before.queuedTasks.nonEmpty || before.scheduledTasks.nonEmpty
        val noResultWaiting = before.completedResults.isEmpty
        val wouldNotChangeAnything = update(before)._1 == before

        if (taskIsInTheQueue && noResultWaiting && wouldNotChangeAnything)
          failures.update(_ + 1) *> IO.raiseError[B](
            new RuntimeException("transient queue state failure")
          )
        else underlying.flatModify(update)
      }
  }

  def run: IO[(Int, Int)] =
    for {
      failures <- Ref.of[IO, Int](0)
      stateRef <- Ref.of[IO, QueueImpl.State](QueueImpl.State.empty)
      transaction = new FailsIdlePolls(
        Transaction.fromRef(stateRef),
        failures
      )
      result <- withTaskSystem(
        config = Some(config),
        s3Client = Resource.pure(None),
        elasticSupport = Resource.pure(None),
        externalQueueState = Resource.pure(Some(transaction))
      ) { implicit ts =>
        slowTask(Input(42))(tasks.ResourceRequest(1, 500))
      }
      observed <- failures.get
    } yield (result.toOption.get, observed)

}

class ProxySurvivesQueueFailureTestSuite extends FunSuite with Matchers {

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Error))
    .replace()

  test(
    "a task still completes when polling the queue for its result transiently fails"
  ) {
    val (result, failures) = ProxySurvivesQueueFailureTest.run
      .unsafeRunTimed(60.seconds)
      .getOrElse(
        fail(
          "the task never completed: the proxy stopped polling after a transient queue failure"
        )
      )

    result shouldBe 42

    withClue("the injected failure path was never exercised: ") {
      failures should be > 0
    }
  }

}
