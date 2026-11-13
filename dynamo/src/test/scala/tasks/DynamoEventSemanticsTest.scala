package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import scala.concurrent.duration._

import tasks.elastic.NodeRegistryState
import tasks.queue.DynamoQueueState
import tasks.queue.Proxy
import tasks.queue.QueueImpl
import tasks.shared.PendingJobId
import tasks.shared.RunningJobId
import tasks.util.message.Address
import tasks.util.message.LauncherName
import tasks.util.message.Node

class DynamoEventSemanticsTest extends FunSuite with Matchers {

  private val tableName = "events-table"

  private def tx = DynamoQueueState.makeTransaction(
    client = new FakeDynamoDbAsyncClient(tableName),
    table = tableName,
    retryBaseDelay = 1.millisecond
  )

  private def runEvents(events: List[QueueImpl.Event]): QueueImpl.State =
    tx.use { t =>
      events.foldLeft(IO.unit) { (acc, event) =>
        acc *> t.flatModify(state => (state.update(event), IO.unit))
      } *> t.get
    }.unsafeRunSync()

  test("Incremented accumulates across commits") {
    val launcher = LauncherName("l-counter")
    val state = runEvents(
      List(
        QueueImpl.LauncherJoined(launcher, None),
        QueueImpl.Incremented(launcher),
        QueueImpl.Incremented(launcher),
        QueueImpl.Incremented(launcher)
      )
    )
    state.counters.get(launcher) shouldBe Some(3L)
  }

  test("LauncherCrashed removes launcher and counter in one commit") {
    val launcher = LauncherName("l-crash")
    val state = runEvents(
      List(
        QueueImpl.LauncherJoined(launcher, None),
        QueueImpl.Incremented(launcher),
        QueueImpl.LauncherCrashed(launcher)
      )
    )
    state.knownLaunchers.get(launcher) shouldBe None
    state.counters.get(launcher) shouldBe None
  }

  test("TaskScheduled a second time appends a second dispatch") {
    val sch = SchedulerFixtures.scheduleTask("replicated", "r-hash")
    val proxy = Proxy(Address("p-rep"))
    val launcherA = LauncherName("l-a")
    val launcherB = LauncherName("l-b")
    val allocated = SchedulerFixtures.allocated()
    val state = runEvents(
      List(
        QueueImpl.Enqueued(sch, List(proxy), 0L),
        QueueImpl.TaskScheduled(sch, launcherA, allocated, None),
        QueueImpl.TaskScheduled(sch, launcherB, allocated, None)
      )
    )
    val scheduled = state.scheduledTasks.values.head
    scheduled.dispatches.map(_.launcher).toSet shouldBe Set(
      launcherA,
      launcherB
    )
  }

  test("TaskDone on one of two dispatches leaves the task scheduled") {
    val sch = SchedulerFixtures.scheduleTask("twosies", "t-hash")
    val proxy = Proxy(Address("p-two"))
    val launcherA = LauncherName("l-two-a")
    val launcherB = LauncherName("l-two-b")
    val allocated = SchedulerFixtures.allocated()
    val state = runEvents(
      List(
        QueueImpl.Enqueued(sch, List(proxy), 0L),
        QueueImpl.TaskScheduled(sch, launcherA, allocated, None),
        QueueImpl.TaskScheduled(sch, launcherB, allocated, None),
        SchedulerFixtures.taskDoneEvent(sch, launcherA, allocated)
      )
    )
    val scheduled = state.scheduledTasks.values.head
    scheduled.dispatches.map(_.launcher) shouldBe List(launcherB)
    scheduled.resultDelivered shouldBe true
  }

  test("TaskFailed with completesCaller=false leaves the queued entry alive") {
    val sch = SchedulerFixtures.scheduleTask("fail-noc", "fnc-hash")
    val proxy = Proxy(Address("p-fnc"))
    val launcher = LauncherName("l-fnc")
    val state = runEvents(
      List(
        QueueImpl.Enqueued(sch, List(proxy), 0L),
        QueueImpl.TaskFailed(sch, launcher, completesCaller = false)
      )
    )
    state.queuedTasks.size shouldBe 1
    state.scheduledTasks shouldBe empty
  }

  test("TaskFailed with completesCaller=true clears the queued entry") {
    val sch = SchedulerFixtures.scheduleTask("fail-c", "fc-hash")
    val proxy = Proxy(Address("p-fc"))
    val launcher = LauncherName("l-fc")
    val state = runEvents(
      List(
        QueueImpl.Enqueued(sch, List(proxy), 0L),
        QueueImpl.TaskFailed(sch, launcher, completesCaller = true)
      )
    )
    state.queuedTasks shouldBe empty
    state.scheduledTasks shouldBe empty
  }

  test("TaskLauncherStoppedFor removes a dispatch without clearing the task") {
    val sch = SchedulerFixtures.scheduleTask("stop", "s-hash")
    val proxy = Proxy(Address("p-stop"))
    val launcherA = LauncherName("l-sa")
    val launcherB = LauncherName("l-sb")
    val allocated = SchedulerFixtures.allocated()
    val state = runEvents(
      List(
        QueueImpl.Enqueued(sch, List(proxy), 0L),
        QueueImpl.TaskScheduled(sch, launcherA, allocated, None),
        QueueImpl.TaskScheduled(sch, launcherB, allocated, None),
        QueueImpl.TaskLauncherStoppedFor(sch, launcherA)
      )
    )
    val scheduled = state.scheduledTasks.values.head
    scheduled.dispatches.map(_.launcher) shouldBe List(launcherB)
    scheduled.resultDelivered shouldBe false
  }

  test("ProxyAddedToScheduledMessage merges additional proxies") {
    val sch = SchedulerFixtures.scheduleTask("merge", "m-hash")
    val launcher = LauncherName("l-merge")
    val allocated = SchedulerFixtures.allocated()
    val proxyA = Proxy(Address("pm-a"))
    val proxyB = Proxy(Address("pm-b"))
    val state = runEvents(
      List(
        QueueImpl.Enqueued(sch, List(proxyA), 0L),
        QueueImpl.TaskScheduled(sch, launcher, allocated, None),
        QueueImpl.ProxyAddedToScheduledMessage(sch, List(proxyB))
      )
    )
    val scheduled = state.scheduledTasks.values.head
    scheduled.proxies.toSet shouldBe Set(proxyA, proxyB)
  }

  test("ResultStoredForProxy then ResultDeliveredToProxy drops the entry") {
    val address = Address("p-store")
    val result = QueueImpl.ProxyResultSuccess(
      SchedulerFixtures.untypedResult(),
      retrievedFromCache = false
    )
    val state = runEvents(
      List(
        QueueImpl.ResultStoredForProxy(address, result),
        QueueImpl.ResultDeliveredToProxy(address)
      )
    )
    state.completedResults shouldBe empty
  }

  test("MainProcessJoined and MainProcessLeft maintain the set") {
    val state = runEvents(
      List(
        QueueImpl.MainProcessJoined("session-a"),
        QueueImpl.MainProcessJoined("session-b"),
        QueueImpl.MainProcessLeft("session-a")
      )
    )
    state.mainProcesses shouldBe Set("session-b")
  }

  test("Enqueueing twice from a second proxy appends but doesn't duplicate") {
    val sch = SchedulerFixtures.scheduleTask("double-enq", "de-hash")
    val proxyA = Proxy(Address("de-a"))
    val proxyB = Proxy(Address("de-b"))
    val state = runEvents(
      List(
        QueueImpl.Enqueued(sch, List(proxyA), 0L),
        QueueImpl.Enqueued(sch, List(proxyB), 0L)
      )
    )
    state.queuedTasks.size shouldBe 1
    state.queuedTasks.values.head._2.toSet shouldBe Set(proxyA, proxyB)
  }

  test("NodeEvent variants round-trip through resources") {
    val resource = SchedulerFixtures.resourceAvailable()
    val node = Node(
      RunningJobId("rj-ne"),
      resource,
      LauncherName("l-ne")
    )
    val pendingId = PendingJobId("pj-ne")
    val state = runEvents(
      List(
        QueueImpl.NodeEvent(NodeRegistryState.NodeRequested(resource)),
        QueueImpl.NodeEvent(
          NodeRegistryState.NodeIsPending(pendingId, resource, resource)
        ),
        QueueImpl.NodeEvent(NodeRegistryState.NodeIsUp(node, pendingId))
      )
    )
    state.nodes.cumulativeRequested shouldBe 1
    state.nodes.running.keySet shouldBe Set(RunningJobId("rj-ne"))
    state.nodes.pending shouldBe empty
  }

  test("MainProcessLeft with clear enabled empties non-task state") {
    val launcher = LauncherName("l-last")
    val state = runEvents(
      List(
        QueueImpl.MainProcessJoined("only"),
        QueueImpl.LauncherJoined(launcher, None),
        QueueImpl.Incremented(launcher),
        QueueImpl.MainProcessLeft("only")
      )
    )
    state.mainProcesses shouldBe empty
    state.knownLaunchers should not be empty
    state.counters.get(launcher) shouldBe Some(1L)
  }

}
