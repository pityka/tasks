package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import tasks.fileservice.FileServicePrefix
import tasks.queue._
import tasks.shared._
import tasks.util.message._

class QueueImplReconcileTestSuite extends FunSuite with Matchers {

  private val launcher = LauncherName("launcher-1")
  private val otherLauncher = LauncherName("launcher-2")

  private val allocated = VersionedResourceAllocated(
    CodeVersion("cv"),
    ResourceAllocated(1, 500, 10, List(0), Some("img"))
  )

  private val sch = MessageData.ScheduleTask(
    description = HashedTaskDescription(TaskId("t", 1), "h"),
    inputDeserializer = Spore[AnyRef, AnyRef]("p.D$", Nil),
    outputSerializer = Spore[AnyRef, AnyRef]("p.S$", Nil),
    function = Spore[AnyRef, AnyRef]("p.J$body$1", Nil),
    resource = VersionedResourceRequest(
      CodeVersion("cv"),
      ResourceRequest((1, 2), 500, 10, 1, Some("img"))
    ),
    input = MessageData.InputData(Base64Data("aGk="), false),
    fileServicePrefix = FileServicePrefix(Vector("p")),
    tryCache = true,
    priority = Priority(0),
    labels = Labels(Nil),
    lineage = TaskLineage(Nil),
    proxy = Address("proxy-1"),
    filePrefix = ""
  )

  private val projection = QueueImpl.project(sch)

  private def scheduledState(dispatchedAtMillis: Option[Long]) =
    QueueImpl.State.empty.copy(
      scheduledTasks = Map(
        projection -> QueueImpl.ScheduledTask(
          sch = sch,
          dispatches =
            List(QueueImpl.Dispatch(launcher, allocated, dispatchedAtMillis)),
          proxies = List(Proxy(Address("proxy-1"))),
          placementValue = None,
          resultDelivered = false
        )
      )
    )

  test("a stale dispatch the launcher no longer runs is re-enqueued") {
    val reconciled = scheduledState(Some(1000L)).reconcileRunning(
      launcher = launcher,
      reportedRunning = Set.empty,
      graceMillis = 100L,
      nowMillis = 2000L
    )

    reconciled.scheduledTasks shouldBe empty
    reconciled.queuedTasks.keySet should contain(projection)
  }

  test("a dispatch the launcher reports running is kept") {
    val reconciled = scheduledState(Some(1000L)).reconcileRunning(
      launcher = launcher,
      reportedRunning = Set(sch.description),
      graceMillis = 100L,
      nowMillis = 2000L
    )

    reconciled.scheduledTasks.keySet should contain(projection)
    reconciled.queuedTasks shouldBe empty
  }

  test("a dispatch still inside the grace window is kept") {
    val reconciled = scheduledState(Some(1000L)).reconcileRunning(
      launcher = launcher,
      reportedRunning = Set.empty,
      graceMillis = 5000L,
      nowMillis = 2000L
    )

    reconciled.scheduledTasks.keySet should contain(projection)
    reconciled.queuedTasks shouldBe empty
  }

  test("a dispatch on a different launcher is not reclaimed") {
    val reconciled = scheduledState(Some(1000L)).reconcileRunning(
      launcher = otherLauncher,
      reportedRunning = Set.empty,
      graceMillis = 100L,
      nowMillis = 2000L
    )

    reconciled.scheduledTasks.keySet should contain(projection)
    reconciled.queuedTasks shouldBe empty
  }

  test("a dispatch from an old snapshot without a timestamp is past grace") {
    val reconciled = scheduledState(None).reconcileRunning(
      launcher = launcher,
      reportedRunning = Set.empty,
      graceMillis = 100L,
      nowMillis = 2000L
    )

    reconciled.scheduledTasks shouldBe empty
    reconciled.queuedTasks.keySet should contain(projection)
  }

}
