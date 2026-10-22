package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import tasks.elastic.NodeRegistryState
import tasks.queue.DynamoCodecs
import tasks.queue.DynamoDiff
import tasks.queue.DynamoQueueState
import tasks.queue.Proxy
import tasks.queue.QueueImpl
import tasks.queue.QueueImpl.State
import tasks.shared.PendingJobId
import tasks.shared.ResourceAvailable
import tasks.util.message.Address
import tasks.util.message.LauncherName

class DynamoQueueStateTest extends FunSuite with Matchers {

  private val tableName = "queue-table"

  private def transactionResource(client: FakeDynamoDbAsyncClient) =
    DynamoQueueState.makeTransaction(
      client = client,
      table = tableName,
      retryBaseDelay = 1.millisecond
    )

  private def newClient(): FakeDynamoDbAsyncClient =
    new FakeDynamoDbAsyncClient(tableName)

  test("an absent resources item is initialized and reads as empty state") {
    val client = newClient()
    val state = transactionResource(client).use(_.get).unsafeRunSync()
    state shouldBe QueueImpl.State.empty
  }

  test("flatModify commits a LauncherJoined and reads it back") {
    val client = newClient()
    val result = transactionResource(client)
      .use { tx =>
        tx.flatModify { state =>
          (
            state.update(QueueImpl.LauncherJoined(LauncherName("l-1"), None)),
            IO.pure(42)
          )
        } *> tx.get
      }
      .unsafeRunSync()
    result.knownLaunchers.keySet shouldBe Set(LauncherName("l-1"))
  }

  test("the side effect runs only after the transact write commits") {
    val client = newClient()
    val sideEffectRan = new AtomicInteger(0)
    val writesAtEffect = new AtomicInteger(-1)
    transactionResource(client)
      .use { tx =>
        tx.flatModify { state =>
          (
            state.update(QueueImpl.LauncherJoined(LauncherName("l-1"), None)),
            IO {
              writesAtEffect.set(client.transactWriteCount.get)
              sideEffectRan.incrementAndGet()
              ()
            }
          )
        }
      }
      .unsafeRunSync()
    sideEffectRan.get shouldBe 1
    writesAtEffect.get shouldBe 1
  }

  test("an update that leaves the state unchanged does not transact-write") {
    val client = newClient()
    val sideEffectRan = new AtomicInteger(0)
    transactionResource(client)
      .use { tx =>
        tx.flatModify { state =>
          (
            state,
            IO {
              sideEffectRan.incrementAndGet()
              ()
            }
          )
        }
      }
      .unsafeRunSync()
    sideEffectRan.get shouldBe 1
    client.transactWriteCount.get shouldBe 0
  }

  test("a competing commit forces a retry and the update is reapplied") {
    val client = newClient()
    val calls = new AtomicInteger(0)
    client.beforeTransactWrite.set { () =>
      if (calls.get == 1) {
        val resources = DynamoDiff.encodeResources(
          knownLaunchers = Map(LauncherName("competitor") -> None),
          counters = Map.empty,
          nodes = NodeRegistryState.State.empty,
          mainProcesses = Set.empty,
          completedCount = Map.empty,
          failedCount = Map.empty,
          ver = 1L
        )
        client.setItem(
          DynamoQueueState.ResourcesPartitionKey,
          DynamoQueueState.ResourcesSortKey,
          resources
        )
      }
    }
    val finalState = transactionResource(client)
      .use { tx =>
        tx.flatModify { state =>
          calls.incrementAndGet()
          (
            state.update(QueueImpl.LauncherJoined(LauncherName("mine"), None)),
            IO.unit
          )
        } *> tx.get
      }
      .unsafeRunSync()
    calls.get shouldBe 2
    finalState.knownLaunchers.keySet shouldBe Set(
      LauncherName("competitor"),
      LauncherName("mine")
    )
  }

  test("Enqueued creates a queued task item") {
    val client = newClient()
    val sch = SchedulerFixtures.scheduleTask("task-a", "hash-1")
    val proxy = Proxy(Address("p-1"))
    val finalState = transactionResource(client)
      .use { tx =>
        tx.flatModify { state =>
          (
            state.update(QueueImpl.Enqueued(sch, List(proxy), 0L)),
            IO.unit
          )
        } *> tx.get
      }
      .unsafeRunSync()
    finalState.queuedTasks.size shouldBe 1
    finalState.queuedTasks.values.head._1.description shouldBe sch.description
    finalState.queuedTasks.values.head._2 shouldBe List(proxy)
  }

  test("TaskScheduled transitions a queued task to scheduled with a dispatch") {
    val client = newClient()
    val sch = SchedulerFixtures.scheduleTask("task-b", "hash-2")
    val launcher = LauncherName("l-worker")
    val allocated = SchedulerFixtures.allocated()
    val proxy = Proxy(Address("p-2"))
    val finalState = transactionResource(client)
      .use { tx =>
        tx.flatModify(state =>
          (state.update(QueueImpl.Enqueued(sch, List(proxy), 0L)), IO.unit)
        ) *>
          tx.flatModify(state =>
            (
              state.update(
                QueueImpl.TaskScheduled(sch, launcher, allocated, None)
              ),
              IO.unit
            )
          ) *> tx.get
      }
      .unsafeRunSync()
    finalState.queuedTasks shouldBe empty
    finalState.scheduledTasks.size shouldBe 1
    val scheduled = finalState.scheduledTasks.values.head
    scheduled.dispatches.map(_.launcher) shouldBe List(launcher)
    scheduled.proxies shouldBe List(proxy)
  }

  test("TaskDone removes the scheduled task item when no dispatches remain") {
    val client = newClient()
    val sch = SchedulerFixtures.scheduleTask("task-c", "hash-3")
    val launcher = LauncherName("l-done")
    val allocated = SchedulerFixtures.allocated()
    val proxy = Proxy(Address("p-3"))
    val result = SchedulerFixtures.taskDoneEvent(sch, launcher, allocated)
    val finalState = transactionResource(client)
      .use { tx =>
        tx.flatModify(state =>
          (state.update(QueueImpl.Enqueued(sch, List(proxy), 0L)), IO.unit)
        ) *>
          tx.flatModify(state =>
            (
              state.update(
                QueueImpl.TaskScheduled(sch, launcher, allocated, None)
              ),
              IO.unit
            )
          ) *>
          tx.flatModify(state => (state.update(result), IO.unit)) *> tx.get
      }
      .unsafeRunSync()
    finalState.scheduledTasks shouldBe empty
    finalState.queuedTasks shouldBe empty
    val taskItems =
      client.rawStore.keys.count(_.pk == DynamoQueueState.TasksPartitionKey)
    taskItems shouldBe 0
  }

  test("CacheHit removes a scheduled task item") {
    val client = newClient()
    val sch = SchedulerFixtures.scheduleTask("task-d", "hash-4")
    val launcher = LauncherName("l-cache")
    val allocated = SchedulerFixtures.allocated()
    val proxy = Proxy(Address("p-4"))
    val cacheResult = SchedulerFixtures.untypedResult()
    val finalState = transactionResource(client)
      .use { tx =>
        tx.flatModify(state =>
          (state.update(QueueImpl.Enqueued(sch, List(proxy), 0L)), IO.unit)
        ) *>
          tx.flatModify(state =>
            (
              state.update(
                QueueImpl.TaskScheduled(sch, launcher, allocated, None)
              ),
              IO.unit
            )
          ) *>
          tx.flatModify(state =>
            (state.update(QueueImpl.CacheHit(sch, cacheResult)), IO.unit)
          ) *> tx.get
      }
      .unsafeRunSync()
    finalState.scheduledTasks shouldBe empty
    val taskItems =
      client.rawStore.keys.count(_.pk == DynamoQueueState.TasksPartitionKey)
    taskItems shouldBe 0
  }

  test(
    "a task-only commit retries when resources moved between read and commit"
  ) {
    val client = newClient()
    val sch = SchedulerFixtures.scheduleTask("barrier-task", "barrier-hash")
    val proxy = Proxy(Address("barrier-proxy"))
    val calls = new AtomicInteger(0)
    client.beforeTransactWrite.set { () =>
      if (calls.get == 1) {
        val resources = DynamoDiff.encodeResources(
          knownLaunchers = Map(LauncherName("race") -> None),
          counters = Map.empty,
          nodes = NodeRegistryState.State.empty,
          mainProcesses = Set.empty,
          completedCount = Map.empty,
          failedCount = Map.empty,
          ver = 1L
        )
        client.setItem(
          DynamoQueueState.ResourcesPartitionKey,
          DynamoQueueState.ResourcesSortKey,
          resources
        )
      }
    }
    val finalState = transactionResource(client)
      .use { tx =>
        tx.flatModify { state =>
          calls.incrementAndGet()
          (state.update(QueueImpl.Enqueued(sch, List(proxy), 0L)), IO.unit)
        } *> tx.get
      }
      .unsafeRunSync()
    calls.get shouldBe 2
    finalState.queuedTasks.size shouldBe 1
    finalState.knownLaunchers.keySet shouldBe Set(LauncherName("race"))
  }

  test(
    "SessionProxiesDropped sweeps proxies and chunks beyond the 100-op cap"
  ) {
    val client = newClient()
    val deadSession = "dead-session"
    val liveProxy = Proxy(Address("live"))
    val deadProxy = Proxy(
      Address(
        tasks.util.SessionId.tag(
          deadSession,
          "Launcher-SimpleSocketAddress(127.0.0.1,1234)"
        )
      )
    )
    val taskCount = 150
    val schList = (0 until taskCount).toList.map(i =>
      SchedulerFixtures.scheduleTask(s"bulk-task-$i", s"bulk-hash-$i")
    )
    val writesBeforeSweep = new AtomicInteger(0)
    val finalState = transactionResource(client)
      .use { tx =>
        schList.foldLeft(IO.unit) { (acc, sch) =>
          acc *> tx.flatModify(state =>
            (
              state.update(
                QueueImpl.Enqueued(sch, List(liveProxy, deadProxy), 0L)
              ),
              IO.unit
            )
          )
        } *>
          IO { writesBeforeSweep.set(client.transactWriteCount.get); () } *>
          tx.flatModify(state =>
            (
              state.update(QueueImpl.SessionProxiesDropped(deadSession)),
              IO.unit
            )
          ) *> tx.get
      }
      .unsafeRunSync()
    finalState.queuedTasks.size shouldBe taskCount
    finalState.queuedTasks.values.foreach { case (_, proxies) =>
      proxies shouldBe List(liveProxy)
    }
    val sweepWrites = client.transactWriteCount.get - writesBeforeSweep.get
    sweepWrites should be > 1
  }

  test("full round-trip of a populated state") {
    val client = newClient()
    val launcher = LauncherName("l-rt")
    val allocated = SchedulerFixtures.allocated()
    val schQ = SchedulerFixtures.scheduleTask("task-rt-q", "hash-q")
    val schS = SchedulerFixtures.scheduleTask("task-rt-s", "hash-s")
    val proxy = Proxy(Address("p-rt"))
    val address = Address("p-stored")
    val result =
      QueueImpl.ProxyResultSuccess(
        SchedulerFixtures.untypedResult(),
        retrievedFromCache = false
      )

    val events: List[QueueImpl.Event] = List(
      QueueImpl.MainProcessJoined("session-1"),
      QueueImpl.LauncherJoined(launcher, None),
      QueueImpl.Incremented(launcher),
      QueueImpl.Enqueued(schQ, List(proxy), 0L),
      QueueImpl.Enqueued(schS, List(proxy), 0L),
      QueueImpl.TaskScheduled(schS, launcher, allocated, Some("rack-A")),
      QueueImpl.ResultStoredForProxy(address, result),
      QueueImpl.NodeEvent(
        NodeRegistryState.NodeRequested(SchedulerFixtures.resourceAvailable())
      )
    )

    val finalState = transactionResource(client)
      .use { tx =>
        events.foldLeft(IO.unit) { (acc, event) =>
          acc *> tx.flatModify(state => (state.update(event), IO.unit))
        } *> tx.get
      }
      .unsafeRunSync()

    finalState.mainProcesses shouldBe Set("session-1")
    finalState.knownLaunchers.keySet shouldBe Set(launcher)
    finalState.counters.get(launcher) shouldBe Some(1L)
    finalState.queuedTasks.values.map(_._1.description).toSet shouldBe Set(
      schQ.description
    )
    finalState.scheduledTasks.values.map(_.sch.description).toSet shouldBe Set(
      schS.description
    )
    finalState.scheduledTasks.values.head.placementValue shouldBe Some("rack-A")
    finalState.completedResults.keySet shouldBe Set(address)
    finalState.nodes.cumulativeRequested shouldBe 1
  }

}
