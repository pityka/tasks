package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.unsafe.implicits.global

import tasks.queue.QueueImpl
import tasks.shared.{CodeVersion, ResourceAvailable, VersionedResourceAvailable}
import tasks.util.LocalMessenger
import tasks.util.message.{LauncherName, MessageData}

class QueueImplAskForWorkTestSuite extends FunSuite with Matchers {

  implicit val config: tasks.util.config.TasksConfig =
    tasks.util.config.parse(() => org.ekrich.config.ConfigFactory.load())

  test(
    "askForWork registers a new launcher even when there is no work to hand it"
  ) {
    val launcher = LauncherName("test-launcher")
    val resource = VersionedResourceAvailable(
      CodeVersion("test"),
      ResourceAvailable(
        cpu = 1,
        memory = 100,
        scratch = 0,
        gpu = Nil,
        image = None
      )
    )

    val program = LocalMessenger.make
      .flatMap { messenger =>
        QueueImpl.initRef(
          cache = null,
          messenger = messenger,
          shutdownNode = None,
          decideNewNode = None,
          createNode = None,
          unmanagedResource = ResourceAvailable.empty,
          meterProvider =
            org.typelevel.otel4s.metrics.MeterProvider.noop[cats.effect.IO]
        )
      }
      .use { q =>
        for {
          before <- q.knownLaunchers
          result <- q.askForWork(launcher, resource, None)
          after <- q.knownLaunchers
        } yield (before, result, after)
      }

    val (before, result, after) = program.unsafeRunSync()

    before.keySet should not contain launcher
    result shouldBe Left(MessageData.NothingForSchedule)
    after.keySet should contain(launcher)
  }

}
