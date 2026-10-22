package tasks.queue

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import tasks.queue.QueueImpl._
import tasks.queue.QueueStateCodecs._
import tasks.shared._
import tasks.util.message.Address
import tasks.util.message.LauncherName
import tasks.util.message.MessageData.ScheduleTask
import tasks.util.message.MessageData.InputData
import tasks.util.transactional.TransactionalStateDiff

class QueueStateDiffTest extends FunSuite with Matchers {

  private val emptySpore =
    Spore[AnyRef, AnyRef](fqcn = "", dependencies = Seq.empty)

  private def scheduleTask(taskId: String, dataHash: String): ScheduleTask =
    ScheduleTask(
      description = HashedTaskDescription(TaskId(taskId, 1), dataHash),
      inputDeserializer = emptySpore,
      outputSerializer = emptySpore,
      function = emptySpore,
      resource = VersionedResourceRequest(
        CodeVersion("v1"),
        ResourceRequest(
          cpu = (1, 1),
          memory = 100,
          scratch = 0,
          gpu = 0,
          image = None,
          nodeSelector = None
        )
      ),
      input = InputData(Base64Data(""), noCache = false),
      fileServicePrefix = tasks.fileservice.FileServicePrefix(Vector.empty),
      tryCache = true,
      priority = Priority(0),
      labels = Labels.empty,
      lineage = TaskLineage.root,
      proxy = Address("test-proxy")
    )

  test("diff of two empty states produces no ops") {
    val d = QueueStateDiff.diff(
      oldState = State.empty,
      newState = State.empty,
      baseTaskVersions = Map.empty,
      baseResultChunkVersions = Map.empty,
      baseResourcesVersion = 0L,
      resourcesSingletonKey = "singleton",
      chunkPayloadBytes = 300 * 1024
    )
    d.isEmpty shouldBe true
  }

  test(
    "diff of a resources-only change emits one Update on the resources singleton"
  ) {
    val newState =
      State.empty.update(QueueImpl.LauncherJoined(LauncherName("l"), None))
    val d = QueueStateDiff.diff(
      oldState = State.empty,
      newState = newState,
      baseTaskVersions = Map.empty,
      baseResultChunkVersions = Map.empty,
      baseResourcesVersion = 7L,
      resourcesSingletonKey = "singleton",
      chunkPayloadBytes = 300 * 1024
    )
    d.taskOps shouldBe empty
    d.resultChunkOps shouldBe empty
    d.resourcesChange shouldBe defined
    d.resourcesChange.get match {
      case TransactionalStateDiff.Update(key, _, baseVer) =>
        key shouldBe "singleton"
        baseVer shouldBe 7L
      case other => fail(s"expected Update, got $other")
    }
  }

  test("diff of a new queued task emits an Insert on the task map") {
    val sch = scheduleTask("ins", "ins-hash")
    val proxy = Proxy(Address("p-ins"))
    val newState = State.empty.update(QueueImpl.Enqueued(sch, List(proxy), 0L))
    val d = QueueStateDiff.diff(
      oldState = State.empty,
      newState = newState,
      baseTaskVersions = Map.empty,
      baseResultChunkVersions = Map.empty,
      baseResourcesVersion = 0L,
      resourcesSingletonKey = "singleton",
      chunkPayloadBytes = 300 * 1024
    )
    d.taskOps.size shouldBe 1
    d.taskOps.head shouldBe a[TransactionalStateDiff.Insert[_]]
    d.taskOps.head.key shouldBe sch.description.hash
    d.resultChunkOps shouldBe empty
  }

  test(
    "diff of a removed task emits a Delete carrying the baseVersion"
  ) {
    val sch = scheduleTask("del", "del-hash")
    val proxy = Proxy(Address("p-del"))
    val oldState = State.empty.update(QueueImpl.Enqueued(sch, List(proxy), 0L))
    val d = QueueStateDiff.diff(
      oldState = oldState,
      newState = State.empty,
      baseTaskVersions = Map(sch.description.hash -> 5L),
      baseResultChunkVersions = Map.empty,
      baseResourcesVersion = 0L,
      resourcesSingletonKey = "singleton",
      chunkPayloadBytes = 300 * 1024
    )
    d.taskOps shouldBe List(
      TransactionalStateDiff.Delete(sch.description.hash, 5L)
    )
  }

  test(
    "diff of an updated task emits an Update carrying the baseVersion"
  ) {
    val sch = scheduleTask("upd", "upd-hash")
    val a = Proxy(Address("p-a"))
    val b = Proxy(Address("p-b"))
    val oldState = State.empty.update(QueueImpl.Enqueued(sch, List(a), 0L))
    val newState = oldState.update(QueueImpl.Enqueued(sch, List(b), 0L))
    val d = QueueStateDiff.diff(
      oldState = oldState,
      newState = newState,
      baseTaskVersions = Map(sch.description.hash -> 9L),
      baseResultChunkVersions = Map.empty,
      baseResourcesVersion = 0L,
      resourcesSingletonKey = "singleton",
      chunkPayloadBytes = 300 * 1024
    )
    d.taskOps.size shouldBe 1
    d.taskOps.head match {
      case TransactionalStateDiff.Update(key, _, baseVer) =>
        key shouldBe sch.description.hash
        baseVer shouldBe 9L
      case other => fail(s"expected Update, got $other")
    }
  }

  test(
    "a ProxyResult larger than chunkPayloadBytes is split across multiple result chunks"
  ) {
    val address = Address("fatty")
    val payloadBytes = 300 * 1024
    val big = new String(
      Array.fill[Byte](payloadBytes * 2 + 100)(0x41.toByte),
      java.nio.charset.StandardCharsets.ISO_8859_1
    )
    val result: ProxyResult =
      QueueImpl.ProxyResultFailure(new RuntimeException(big))
    val newState =
      State.empty.update(QueueImpl.ResultStoredForProxy(address, result))

    val d = QueueStateDiff.diff(
      oldState = State.empty,
      newState = newState,
      baseTaskVersions = Map.empty,
      baseResultChunkVersions = Map.empty,
      baseResourcesVersion = 0L,
      resourcesSingletonKey = "singleton",
      chunkPayloadBytes = payloadBytes
    )
    val keys = d.resultChunkOps.map(_.key).toSet
    keys.size should be > 1
    all(keys) should startWith(address.value + QueueStateDiff.ChunkSeparator)
  }

  test(
    "assembleState and taskItemsOf+resourcesOf are inverses on an interesting state"
  ) {
    val sch1 = scheduleTask("one", "one-hash")
    val sch2 = scheduleTask("two", "two-hash")
    val p1 = Proxy(Address("p1"))
    val p2 = Proxy(Address("p2"))

    val state = State.empty
      .update(QueueImpl.LauncherJoined(LauncherName("l"), None))
      .update(QueueImpl.MainProcessJoined("s"))
      .update(QueueImpl.Enqueued(sch1, List(p1), 11L))
      .update(QueueImpl.Enqueued(sch2, List(p2), 22L))

    val taskMap = QueueStateDiff.taskItemsOf(state).map { case (k, v) =>
      k -> ((v.sch, v, 0L))
    }
    val roundTripped = QueueStateDiff.assembleState(
      taskMap = taskMap,
      completedResults = Map.empty,
      resources = QueueStateDiff.resourcesOf(state)
    )
    roundTripped shouldBe state
  }

  test(
    "completedResultChunks + assembleCompletedResults is a lossless round-trip for ProxyResultSuccess"
  ) {
    val address = Address("rt")
    val result: ProxyResult = QueueImpl.ProxyResultSuccess(
      tasks.queue.UntypedResult(
        files = Set.empty,
        data = tasks.queue.Base64Data("abc"),
        mutableFiles = None
      ),
      retrievedFromCache = false
    )
    val chunks =
      QueueStateDiff.completedResultChunks(Map(address -> result), 300 * 1024)
    val withVersions = chunks.map { case (sk, bytes) =>
      sk -> ((bytes, 1L))
    }
    val reassembled = QueueStateDiff.assembleCompletedResults(withVersions)
    reassembled shouldBe Map(address -> result)
  }

  test(
    "chunkSk and parseAddressFromSk are inverses for any index"
  ) {
    val address = "proxy/addr"
    (0 to 10).foreach { i =>
      val sk = QueueStateDiff.chunkSk(address, i)
      QueueStateDiff.parseAddressFromSk(sk) shouldBe address
    }
  }

  test(
    "ChunkSeparator keeps lexicographic chunk ordering within an address"
  ) {
    val a = QueueStateDiff.chunkSk("same", 0)
    val b = QueueStateDiff.chunkSk("same", 1)
    (a < b) shouldBe true
  }

}
