package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import tasks.elastic.NodeRegistryState
import tasks.fileservice.FileServicePrefix
import tasks.queue._
import tasks.shared._
import tasks.util.message._

class RunningJobAttributionTest extends FunSuite with Matchers {

  private val cv = CodeVersion("test")

  private def launcher(name: String) = LauncherName(name)

  private def node(name: String, launcher: LauncherName) =
    Node(
      name = RunningJobId(name),
      size = ResourceAvailable(
        cpu = 4,
        memory = 2000,
        scratch = 0,
        gpu = Nil,
        image = None
      ),
      launcherActor = launcher
    )

  private def scheduleTask(hash: String) =
    MessageData.ScheduleTask(
      description = HashedTaskDescription(TaskId("t", 1), hash),
      inputDeserializer = Spore[AnyRef, AnyRef]("some.pkg.Deserializer$", Nil),
      outputSerializer = Spore[AnyRef, AnyRef]("some.pkg.Serializer$", Nil),
      function = Spore[AnyRef, AnyRef]("some.pkg.Jobs$body$1", Nil),
      resource = VersionedResourceRequest(
        cv,
        ResourceRequest((1, 1), 500, 0, 0, None)
      ),
      input = MessageData.InputData(Base64Data("aGVsbG8="), false),
      fileServicePrefix = FileServicePrefix(Vector("prefix")),
      tryCache = true,
      priority = Priority(0),
      labels = Labels(Nil),
      lineage = TaskLineage(Nil),
      proxy = Address("proxy-" + hash, None),
      filePrefix = "file-prefix"
    )

  private def scheduled(
      hash: String,
      onLauncher: LauncherName,
      cpu: Int,
      memory: Int
  ) = {
    val sch = scheduleTask(hash)
    QueueImpl.project(sch) -> (
      (
        onLauncher,
        VersionedResourceAllocated(
          cv,
          ResourceAllocated(
            cpu = cpu,
            memory = memory,
            scratch = 0,
            gpu = Nil,
            image = None
          )
        ),
        List.empty[Proxy],
        sch
      )
    )
  }

  test(
    "a running task is deducted from the node its launcher actually runs on, and a task on a launcher with no node is deducted from nothing"
  ) {
    val launcherA = launcher("launcher-a")
    val launcherB = launcher("launcher-b")
    val masterLauncher = launcher("launcher-master")

    val nodeA = node("node-a", launcherA)
    val nodeB = node("node-b", launcherB)

    val state = QueueImpl.State(
      queuedTasks = Map.empty,
      scheduledTasks = Map(
        scheduled("onA", launcherA, cpu = 2, memory = 1000),
        scheduled("onMaster", masterLauncher, cpu = 3, memory = 1500)
      ),
      knownLaunchers = Map(
        launcherA -> Some(nodeA),
        launcherB -> Some(nodeB),
        masterLauncher -> None
      ),
      counters = Map.empty,
      nodes = NodeRegistryState.State.empty.copy(
        running = Map(
          RunningJobId("node-a") -> nodeA.size,
          RunningJobId("node-b") -> nodeB.size
        )
      ),
      rendezvous = Map.empty,
      completedResults = Map.empty,
      mainProcesses = Set.empty
    )

    val free = state.freeCapacityOfRunningNodes.toSet

    free shouldBe Set(
      ResourceAvailable(
        cpu = 2,
        memory = 1000,
        scratch = 0,
        gpu = Nil,
        image = None
      ),
      ResourceAvailable(
        cpu = 4,
        memory = 2000,
        scratch = 0,
        gpu = Nil,
        image = None
      )
    )
  }

}
