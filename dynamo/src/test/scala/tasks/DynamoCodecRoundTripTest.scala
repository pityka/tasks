package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import tasks.elastic.NodeRegistryState
import tasks.queue.DynamoCodecs._
import tasks.queue.DynamoDiff
import tasks.queue.DynamoQueueState
import tasks.queue.QueueStateDiff
import tasks.queue.Proxy
import tasks.queue.QueueImpl
import tasks.shared._
import tasks.util.message.Address
import tasks.util.message.LauncherName
import tasks.util.message.MessageData.ScheduleTask
import tasks.util.message.Node

class DynamoCodecRoundTripTest extends FunSuite with Matchers {

  private def sampleSchedule(id: String): ScheduleTask =
    SchedulerFixtures.scheduleTask(id, s"$id-hash")

  test("ScheduleTask round-trips through jsoniter") {
    val original = sampleSchedule("rt-1")
    val bytes = encode(original)
    val decoded = decode[ScheduleTask](bytes)
    decoded shouldBe original
  }

  test("Proxy round-trips through jsoniter") {
    val original = Proxy(Address("p-1", Some("http://host:1234")))
    val bytes = encode(original)
    decode[Proxy](bytes) shouldBe original
  }

  test("Dispatch round-trips through jsoniter") {
    val original = QueueImpl.Dispatch(
      launcher = LauncherName("l-1"),
      allocated = SchedulerFixtures.allocated(cpu = 2, memory = 500)
    )
    val bytes = encode(original)
    decode[QueueImpl.Dispatch](bytes) shouldBe original
  }

  test("ProxyResultSuccess round-trips") {
    val original: QueueImpl.ProxyResult =
      QueueImpl.ProxyResultSuccess(
        SchedulerFixtures.untypedResult(),
        retrievedFromCache = true
      )
    val bytes = encode(original)
    decode[QueueImpl.ProxyResult](bytes) shouldBe original
  }

  test("ProxyResultFailure round-trips preserving the message") {
    val cause = new RuntimeException("boom")
    val original: QueueImpl.ProxyResult = QueueImpl.ProxyResultFailure(cause)
    val bytes = encode(original)
    val decoded = decode[QueueImpl.ProxyResult](bytes)
    decoded shouldBe a[QueueImpl.ProxyResultFailure]
    decoded
      .asInstanceOf[QueueImpl.ProxyResultFailure]
      .cause
      .getMessage shouldBe "boom"
  }

  test("Option[Node] round-trips for Some and None") {
    val none = decode[Option[Node]](encode[Option[Node]](None))
    none shouldBe None
    val node = Node(
      name = RunningJobId("job-1"),
      size = SchedulerFixtures.resourceAvailable(cpu = 8, memory = 2000),
      launcherActor = LauncherName("l-a")
    )
    decode[Option[Node]](encode[Option[Node]](Some(node))) shouldBe Some(node)
  }

  test("NodeRegistryState.State with populated maps round-trips") {
    val original = NodeRegistryState.State.empty
      .update(
        NodeRegistryState.NodeRequested(SchedulerFixtures.resourceAvailable())
      )
      .update(
        NodeRegistryState.NodeIsPending(
          PendingJobId("p-1"),
          SchedulerFixtures.resourceAvailable(),
          SchedulerFixtures.resourceAvailable()
        )
      )
      .update(
        NodeRegistryState.NodeIsUp(
          Node(
            RunningJobId("r-1"),
            SchedulerFixtures.resourceAvailable(),
            LauncherName("l-r")
          ),
          PendingJobId("p-1")
        )
      )
    val bytes = encode(original)
    decode[NodeRegistryState.State](bytes) shouldBe original
  }

  test("task item encode/decode preserves every field") {
    val fields = DynamoDiff.TaskItemFields(
      sch = sampleSchedule("rt-2"),
      status = QueueStateDiff.StatusScheduled,
      proxies = List(Proxy(Address("p-a")), Proxy(Address("p-b"))),
      dispatches = List(
        QueueImpl.Dispatch(LauncherName("l-x"), SchedulerFixtures.allocated()),
        QueueImpl
          .Dispatch(LauncherName("l-y"), SchedulerFixtures.allocated(cpu = 4))
      ),
      placementValue = Some("rack-7"),
      resultDelivered = true,
      enqueueTimestamp = None
    )
    val attrs = DynamoDiff.encodeTaskAttrs("sk-1", fields, ver = 42L)
    val javaAttrs =
      scala.jdk.CollectionConverters.MapHasAsJava(attrs).asJava
    val (sk, (sch, decodedFields, ver)) = DynamoDiff.decodeTaskItem(javaAttrs)
    sk shouldBe "sk-1"
    ver shouldBe 42L
    sch shouldBe fields.sch
    decodedFields shouldBe fields
  }

  test("task item encode/decode with placementValue=None omits the attribute") {
    val fields = DynamoDiff.TaskItemFields(
      sch = sampleSchedule("rt-3"),
      status = QueueStateDiff.StatusQueued,
      proxies = Nil,
      dispatches = Nil,
      placementValue = None,
      resultDelivered = false,
      enqueueTimestamp = Some(1234567L)
    )
    val attrs = DynamoDiff.encodeTaskAttrs("sk-2", fields, ver = 1L)
    attrs.contains(DynamoQueueState.Attr.PlacementValue) shouldBe false
    val javaAttrs =
      scala.jdk.CollectionConverters.MapHasAsJava(attrs).asJava
    val (_, (_, decodedFields, _)) = DynamoDiff.decodeTaskItem(javaAttrs)
    decodedFields.placementValue shouldBe None
  }

  test("resources item encode/decode preserves every field") {
    val launcherA = LauncherName("l-ra")
    val launcherB = LauncherName("l-rb")
    val node = Node(
      RunningJobId("job-x"),
      SchedulerFixtures.resourceAvailable(cpu = 16, memory = 4000),
      LauncherName("l-r")
    )
    val nodes = NodeRegistryState.State.empty
      .update(
        NodeRegistryState.NodeRequested(SchedulerFixtures.resourceAvailable())
      )
      .update(
        NodeRegistryState.NodeIsPending(
          PendingJobId("p-9"),
          SchedulerFixtures.resourceAvailable(),
          SchedulerFixtures.resourceAvailable()
        )
      )
    val attrs = DynamoDiff.encodeResources(
      knownLaunchers = Map(launcherA -> Some(node), launcherB -> None),
      counters = Map(launcherA -> 7L, launcherB -> 11L),
      nodes = nodes,
      mainProcesses = Set("session-x", "session-y"),
      completedCount = Map.empty,
      failedCount = Map.empty,
      ver = 99L
    )
    val javaAttrs =
      scala.jdk.CollectionConverters.MapHasAsJava(attrs).asJava
    val (fields, ver) = DynamoDiff.decodeResources(javaAttrs)
    ver shouldBe 99L
    fields.knownLaunchers shouldBe Map(
      launcherA -> Some(node),
      launcherB -> None
    )
    fields.counters shouldBe Map(launcherA -> 7L, launcherB -> 11L)
    fields.nodes shouldBe nodes
    fields.mainProcesses shouldBe Set("session-x", "session-y")
  }

}
