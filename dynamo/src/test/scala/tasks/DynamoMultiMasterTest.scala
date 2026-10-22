package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import tasks.queue.DynamoQueueState
import tasks.queue.Proxy
import tasks.queue.QueueImpl
import tasks.util.message.Address
import tasks.util.message.LauncherName

class DynamoMultiMasterTest extends FunSuite with Matchers {

  private val tableName = "mm-table"

  private def pair(
      client: FakeDynamoDbAsyncClient
  ) = {
    val a = DynamoQueueState.makeTransaction(
      client = client,
      table = tableName,
      retryBaseDelay = 1.millisecond
    )
    val b = DynamoQueueState.makeTransaction(
      client = client,
      table = tableName,
      retryBaseDelay = 1.millisecond
    )
    a.flatMap(txA => b.map(txB => (txA, txB)))
  }

  test("two transactions see each other's committed launcher joins") {
    val client = new FakeDynamoDbAsyncClient(tableName)
    val finalCombined = pair(client)
      .use { case (txA, txB) =>
        txA.flatModify(state =>
          (
            state.update(QueueImpl.LauncherJoined(LauncherName("a"), None)),
            IO.unit
          )
        ) *>
          txB.flatModify(state =>
            (
              state.update(QueueImpl.LauncherJoined(LauncherName("b"), None)),
              IO.unit
            )
          ) *> txA.get
      }
      .unsafeRunSync()
    finalCombined.knownLaunchers.keySet shouldBe Set(
      LauncherName("a"),
      LauncherName("b")
    )
  }

  test(
    "two concurrent flatModify calls both commit — resources ver advances twice"
  ) {
    val client = new FakeDynamoDbAsyncClient(tableName)
    val finalState = pair(client)
      .use { case (txA, txB) =>
        val joinA = txA.flatModify(state =>
          (
            state.update(QueueImpl.LauncherJoined(LauncherName("ca"), None)),
            IO.unit
          )
        )
        val joinB = txB.flatModify(state =>
          (
            state.update(QueueImpl.LauncherJoined(LauncherName("cb"), None)),
            IO.unit
          )
        )
        joinA.both(joinB) *> txA.get
      }
      .unsafeRunSync()
    finalState.knownLaunchers.keySet shouldBe Set(
      LauncherName("ca"),
      LauncherName("cb")
    )
  }

  test(
    "dispatch race: both masters try to schedule the same queued task, exactly one dispatch lands"
  ) {
    val client = new FakeDynamoDbAsyncClient(tableName)
    val sch = SchedulerFixtures.scheduleTask("race", "race-hash")
    val proxy = Proxy(Address("p-race"))
    val launcherA = LauncherName("l-ra")
    val launcherB = LauncherName("l-rb")
    val allocated = SchedulerFixtures.allocated()
    val scheduleAttempts = new AtomicInteger(0)

    val finalState = pair(client)
      .use { case (txA, txB) =>
        txA.flatModify(state =>
          (state.update(QueueImpl.Enqueued(sch, List(proxy), 0L)), IO.unit)
        ) *> {
          val scheduleA = txA.flatModify { state =>
            scheduleAttempts.incrementAndGet()
            val next =
              if (state.scheduledTasks.contains(QueueImpl.project(sch))) state
              else
                state.update(
                  QueueImpl.TaskScheduled(sch, launcherA, allocated, None)
                )
            (next, IO.unit)
          }
          val scheduleB = txB.flatModify { state =>
            scheduleAttempts.incrementAndGet()
            val next =
              if (state.scheduledTasks.contains(QueueImpl.project(sch))) state
              else
                state.update(
                  QueueImpl.TaskScheduled(sch, launcherB, allocated, None)
                )
            (next, IO.unit)
          }
          scheduleA.both(scheduleB)
        } *> txA.get
      }
      .unsafeRunSync()

    finalState.scheduledTasks.size shouldBe 1
    val scheduled = finalState.scheduledTasks.values.head
    scheduled.dispatches.size shouldBe 1
    Set(launcherA, launcherB) should contain(scheduled.dispatches.head.launcher)
  }

  test(
    "resources ConditionCheck forces a retry when a competitor commits first"
  ) {
    val client = new FakeDynamoDbAsyncClient(tableName)
    val sch = SchedulerFixtures.scheduleTask("bar", "bar-hash")
    val proxy = Proxy(Address("p-bar"))
    val attempts = new AtomicInteger(0)

    val finalState = pair(client)
      .use { case (txA, txB) =>
        // Prime both caches so they both believe they are on resources ver=0.
        txA.get *> txB.get *>
          // Competitor commits resources change first.
          txB.flatModify(state =>
            (
              state.update(
                QueueImpl.LauncherJoined(LauncherName("rival"), None)
              ),
              IO.unit
            )
          ) *>
          // Now our task-only commit will see stale resources ver, retry, succeed.
          txA.flatModify { state =>
            attempts.incrementAndGet()
            (state.update(QueueImpl.Enqueued(sch, List(proxy), 0L)), IO.unit)
          } *> txA.get
      }
      .unsafeRunSync()

    attempts.get should be >= 2
    finalState.queuedTasks.size shouldBe 1
    finalState.knownLaunchers.keySet shouldBe Set(LauncherName("rival"))
  }

}
