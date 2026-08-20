package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.kernel.Deferred
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import org.ekrich.config.ConfigFactory
import scala.concurrent.duration._

import tasks.jsonitersupport._
import tasks.queue._

object EagerLauncherRegistrationTest extends TestHelpers {

  val testTask = tasks.Task[Input, Int]("eagerLauncherRegistration", 1) {
    in => _ => IO(in.i)
  }

  val config = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      tasks.cache.enabled = false
      hosts.numCPU = 2
      tasks.disableRemoting = false
      tasks.addShutdownHook = false
      tasks.askInterval = 2 seconds
      """
    )
  }

  private def taskSystem(
      queueStateRef: Ref[IO, QueueImpl.State]
  ): Resource[IO, TaskSystemComponents] =
    Resource
      .eval(Deferred[IO, cats.effect.ExitCode])
      .flatMap { exitCode =>
        tasks.defaultTaskSystem(
          config = Some(config),
          s3Client = Resource.pure(None),
          elasticSupport = Resource.pure(None),
          externalQueueState =
            Resource.pure(Some(tasks.util.Transaction.fromRef(queueStateRef))),
          exitCode = exitCode
        )
      }
      .map(_._1)

  def run: IO[(QueueImpl.State, Int)] =
    Ref.of[IO, QueueImpl.State](QueueImpl.State.empty).flatMap {
      queueStateRef =>
        taskSystem(queueStateRef).use { implicit ts =>
          for {
            atHandover <- queueStateRef.get
            result <- testTask(Input(7))(tasks.ResourceRequest(1, 500))
          } yield (atHandover, result)
        }
    }

}

class EagerLauncherRegistrationTestSuite extends FunSuite with Matchers {

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Warn))
    .replace()

  test(
    "the local launcher is registered in the queue state before user code runs, so a task submitted immediately does not see an empty launcher set"
  ) {
    val (atHandover, result) = EagerLauncherRegistrationTest.run
      .unsafeRunTimed(90.seconds)
      .getOrElse(throw new RuntimeException("timeout"))

    atHandover.knownLaunchers should have size 1

    result shouldBe 7
  }

}
