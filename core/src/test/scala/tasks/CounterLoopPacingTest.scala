package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global
import org.ekrich.config.ConfigFactory

import scala.concurrent.duration._

import tasks.queue.QueueImpl
import tasks.shared.ResourceAvailable
import tasks.util.LocalMessenger
import tasks.util.Transaction

class CounterLoopPacingTest extends FunSuite with Matchers {

  private val heartbeat = 200.milliseconds

  private implicit val config: tasks.util.config.TasksConfig =
    tasks.util.config.parse(() =>
      tasks.util.loadConfig(
        Some(
          ConfigFactory.parseString(
            "tasks.failuredetector.heartbeat-interval = 200 millis"
          )
        )
      )
    )

  private def countingTransaction(
      reads: Ref[IO, Int],
      answer: IO[QueueImpl.State]
  ): Transaction[QueueImpl.State] =
    new Transaction[QueueImpl.State] {
      def get: IO[QueueImpl.State] = reads.update(_ + 1) *> answer
      def flatModify[B](
          update: QueueImpl.State => (QueueImpl.State, IO[B])
      ): IO[B] =
        answer.flatMap(state => update(state)._2)
    }

  private def withoutLogging[A](body: => A): A = {
    scribe.Logger.root.clearHandlers().clearModifiers().replace()
    try body
    finally {
      scribe.Logger.root
        .clearHandlers()
        .clearModifiers()
        .withHandler(minimumLevel = Some(scribe.Level.Warn))
        .replace()
    }
  }

  private def readsDuring(
      observationWindow: FiniteDuration,
      answer: IO[QueueImpl.State]
  ): Int = withoutLogging {
    val program = Ref.of[IO, Int](0).flatMap { reads =>
      LocalMessenger.make
        .flatMap { messenger =>
          QueueImpl.fromTransaction(
            transaction = countingTransaction(reads, answer),
            cache = null,
            messenger = messenger,
            shutdownNode = None,
            decideNewNode = None,
            createNode = None,
            convertRunningToPending = None,
            unmanagedResource = ResourceAvailable.empty,
            meterProvider = org.typelevel.otel4s.metrics.MeterProvider.noop[IO],
            mainProcessSession = None
          )
        }
        .use(_ => IO.sleep(observationWindow))
        .flatMap(_ => reads.get)
    }
    program.unsafeRunTimed(observationWindow + 30.seconds).get
  }

  test(
    "the counter loop does not spin when the queue state cannot be read"
  ) {
    val window = 2.seconds
    val reads = readsDuring(
      window,
      IO.raiseError(new RuntimeException("state backend is unreachable"))
    )

    val roundsInWindow = (window / heartbeat).toInt

    withClue(s"reads=$reads over $window with heartbeat $heartbeat: ") {
      reads should be <= (roundsInWindow * 4)
    }
  }

  test(
    "the counter loop does not spin when no launcher is known"
  ) {
    val window = 2.seconds
    val reads = readsDuring(window, IO.pure(QueueImpl.State.empty))

    val roundsInWindow = (window / heartbeat).toInt

    withClue(s"reads=$reads over $window with heartbeat $heartbeat: ") {
      reads should be <= (roundsInWindow * 4)
    }
  }

}
