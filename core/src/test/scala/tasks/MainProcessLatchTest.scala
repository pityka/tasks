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
import tasks.queue._

object MainProcessLatchTest extends TestHelpers {

  val testTask = tasks.Task[Input, Int]("mainProcessLatch", 1) { in => _ =>
    IO(in.i)
  }

  def config(clearWhenLastExits: Boolean) = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      tasks.cache.enabled = false
      hosts.numCPU = 2
      tasks.disableRemoting = false
      tasks.addShutdownHook = false
      tasks.clearQueueStateWhenLastMainProcessExits = $clearWhenLastExits
      """
    )
  }

  def mainProcess(
      queueStateRef: Ref[IO, QueueImpl.State]
  ): Resource[IO, TaskSystemComponents] = mainProcess(queueStateRef, true)

  def mainProcess(
      queueStateRef: Ref[IO, QueueImpl.State],
      clearWhenLastExits: Boolean
  ): Resource[IO, TaskSystemComponents] =
    Resource
      .eval(cats.effect.kernel.Deferred[IO, cats.effect.ExitCode])
      .flatMap { exitCode =>
        tasks.defaultTaskSystem(
          config = Some(config(clearWhenLastExits)),
          s3Client = Resource.pure(None),
          elasticSupport = Resource.pure(None),
          externalQueueState =
            Resource.pure(Some(tasks.util.Transaction.fromRef(queueStateRef))),
          exitCode = exitCode
        )
      }
      .map(_._1)

}

class MainProcessLatchTestSuite extends FunSuite with Matchers {

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Warn))
    .replace()

  import MainProcessLatchTest._

  test(
    "with clearing disabled the last main process to leave keeps the queue state, so a task it left queued survives for a later run"
  ) {
    val program = Ref.of[IO, QueueImpl.State](QueueImpl.State.empty).flatMap {
      queueStateRef =>
        for {
          _ <- queueStateRef.update(state =>
            state.copy(nodes = state.nodes.copy(cumulativeRequested = 102))
          )
          _ <- mainProcess(queueStateRef, clearWhenLastExits = false).use {
            implicit ts =>
              testTask(Input(1))(tasks.ResourceRequest(1, 500)).void
          }
          afterShutdown <- queueStateRef.get
        } yield afterShutdown
    }

    val afterShutdown = program
      .unsafeRunTimed(90.seconds)
      .getOrElse(throw new RuntimeException("timeout"))

    afterShutdown should not be QueueImpl.State.empty
    afterShutdown.nodes.cumulativeRequested shouldBe 102
    afterShutdown.mainProcesses shouldBe empty
  }

  test(
    "by default the last main process to leave empties the queue state, and a leftover cumulative node budget goes with it"
  ) {
    val program = Ref.of[IO, QueueImpl.State](QueueImpl.State.empty).flatMap {
      queueStateRef =>
        for {
          _ <- queueStateRef.update(state =>
            state.copy(nodes = state.nodes.copy(cumulativeRequested = 102))
          )
          whileRunning <- mainProcess(queueStateRef).use { implicit ts =>
            testTask(Input(1))(tasks.ResourceRequest(1, 500)) *>
              queueStateRef.get
          }
          afterShutdown <- queueStateRef.get
        } yield (whileRunning, afterShutdown)
    }

    val (whileRunning, afterShutdown) = program
      .unsafeRunTimed(90.seconds)
      .getOrElse(throw new RuntimeException("timeout"))

    whileRunning.mainProcesses should have size 1
    whileRunning.knownLaunchers should not be empty
    whileRunning.nodes.cumulativeRequested shouldBe 102

    afterShutdown shouldBe QueueImpl.State.empty
  }

  test(
    "a main process leaving while another is still running leaves the queue state intact"
  ) {
    val program = Ref.of[IO, QueueImpl.State](QueueImpl.State.empty).flatMap {
      queueStateRef =>
        for {
          _ <- queueStateRef.update(state =>
            state.copy(nodes = state.nodes.copy(cumulativeRequested = 7))
          )
          afterFirstLeft <- mainProcess(queueStateRef).use { _ =>
            for {
              bothUp <- mainProcess(queueStateRef).use(_ => queueStateRef.get)
              afterFirstLeft <- queueStateRef.get
            } yield (bothUp, afterFirstLeft)
          }
          afterBothLeft <- queueStateRef.get
        } yield (afterFirstLeft._1, afterFirstLeft._2, afterBothLeft)
    }

    val (bothUp, afterFirstLeft, afterBothLeft) = program
      .unsafeRunTimed(90.seconds)
      .getOrElse(throw new RuntimeException("timeout"))

    bothUp.mainProcesses should have size 2
    bothUp.nodes.cumulativeRequested shouldBe 7

    afterFirstLeft.mainProcesses should have size 1
    afterFirstLeft.nodes.cumulativeRequested shouldBe 7

    afterBothLeft shouldBe QueueImpl.State.empty
  }

}
