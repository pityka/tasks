package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import tasks.elastic.NodeRegistryState
import tasks.fileservice.FileServicePrefix
import tasks.queue._
import tasks.shared._
import tasks.util.message._

class SerializableQueueStateTest extends FunSuite with Matchers {

  private val resource =
    ResourceAvailable(cpu = 2, memory = 500, scratch = 10, gpu = List(0), image = Some("img"))

  private val launcher = LauncherName("launcher-1")

  private val node = Node(RunningJobId("running-1"), resource, launcher)

  private val proxyAddress =
    Address("ProxyTask-name.1-12345-abcdef", Some("http://worker:1234/prefix"))

  private def scheduleTask(name: String, hash: String) =
    MessageData.ScheduleTask(
      description = HashedTaskDescription(TaskId(name, 1), hash),
      inputDeserializer = Spore[AnyRef, AnyRef]("some.pkg.Deserializer$", Nil),
      outputSerializer = Spore[AnyRef, AnyRef]("some.pkg.Serializer$", Nil),
      function = Spore[AnyRef, AnyRef](
        "some.pkg.Jobs$body$1",
        Seq(Spore[Any, Any]("some.pkg.Dependency$", Nil))
      ),
      resource = VersionedResourceRequest(
        CodeVersion("code-version-1"),
        ResourceRequest((1, 2), 500, 10, 1, Some("img"))
      ),
      input = MessageData.InputData(Base64Data("aGVsbG8gd29ybGQ="), false),
      fileServicePrefix = FileServicePrefix(Vector("prefix-a", "prefix-b")),
      tryCache = true,
      priority = Priority(7),
      labels = Labels(List("key" -> "value")),
      lineage = TaskLineage(
        Seq(
          TaskInvocationId(
            TaskId("parent", 3),
            HashedTaskDescription(TaskId("parent", 3), "parent-hash")
          )
        )
      ),
      proxy = proxyAddress,
      filePrefix = "file-prefix"
    )

  private val queued = scheduleTask("queued-task", "queued-hash")
  private val scheduled = scheduleTask("scheduled-task", "scheduled-hash")

  private val state = QueueImpl.State(
    queuedTasks = Map(
      QueueImpl.project(queued) -> ((queued, List(Proxy(proxyAddress))))
    ),
    scheduledTasks = Map(
      QueueImpl.project(scheduled) -> QueueImpl.ScheduledTask(
        sch = scheduled,
        dispatches = List(
          QueueImpl.Dispatch(
            launcher,
            VersionedResourceAllocated(
              CodeVersion("code-version-1"),
              ResourceAllocated(1, 500, 10, List(0), Some("img"))
            )
          ),
          QueueImpl.Dispatch(
            LauncherName("launcher-2"),
            VersionedResourceAllocated(
              CodeVersion("code-version-1"),
              ResourceAllocated(1, 500, 10, List(0), Some("img"))
            )
          )
        ),
        proxies = List(Proxy(proxyAddress)),
        placementValue = Some("eu-west-1a"),
        resultDelivered = false
      )
    ),
    knownLaunchers = Map(launcher -> Some(node)),
    counters = Map(launcher -> 42L),
    nodes = NodeRegistryState.State(
      running = Map(RunningJobId("running-1") -> resource),
      pending = Map(PendingJobId("pending-1") -> resource),
      cumulativeRequested = 5,
      inFlightRequests = List(resource)
    ),
    completedResults = Map.empty,
    mainProcesses = Set.empty
  )

  test("a populated queue state survives an encode/decode round trip") {
    val decoded =
      SerializableQueueState.decode(SerializableQueueState.encode(state))

    decoded shouldBe state
  }

  test("the round trip preserves proxy listening uris") {
    val decoded =
      SerializableQueueState.decode(SerializableQueueState.encode(state))

    val decodedProxies =
      decoded.queuedTasks.values.flatMap(_._2).map(_.address).toList

    decodedProxies.map(_.listeningUri) shouldBe List(
      Some("http://worker:1234/prefix")
    )
    decoded.queuedTasks.values.map(_._1.proxy.listeningUri).toList shouldBe List(
      Some("http://worker:1234/prefix")
    )
  }

  test("a completed failure result round trips with message and stack trace") {
    val cause = new RuntimeException("boom")
    val withResult = state.copy(completedResults =
      Map(proxyAddress -> QueueImpl.ProxyResultFailure(cause))
    )

    val decoded =
      SerializableQueueState.decode(SerializableQueueState.encode(withResult))

    decoded.completedResults.keySet shouldBe Set(proxyAddress)
    decoded.completedResults(proxyAddress) match {
      case QueueImpl.ProxyResultFailure(decodedCause) =>
        decodedCause.getMessage shouldBe "boom"
        decodedCause.getStackTrace.toList should not be empty
      case other => fail(s"expected a failure result, got $other")
    }
  }

  test("the empty state round trips") {
    SerializableQueueState.decode(
      SerializableQueueState.encode(QueueImpl.State.empty)
    ) shouldBe QueueImpl.State.empty

    SerializableQueueState.decode(
      SerializableQueueState.emptyStateAsString
    ) shouldBe QueueImpl.State.empty
  }

}
