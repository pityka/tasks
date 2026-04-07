package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import tasks.elastic.SimpleDecideNewNode
import tasks.shared._
import tasks.util.message.QueueStat

class SimpleDecideNewNodeTestSuite extends FunSuite with Matchers {

  val cv = CodeVersion("test")

  implicit val config: tasks.util.config.TasksConfig =
    tasks.util.config.parse(() => org.ekrich.config.ConfigFactory.load())

  val decider = new SimpleDecideNewNode(cv)

  test(
    "should not request new nodes when existing nodes can handle queued tasks"
  ) {
    val queuedRequest = ResourceRequest((2, 2), 1000, 0, 0, None)
    val queueStat = QueueStat(
      queued = List(
        ("task1", VersionedResourceRequest(cv, queuedRequest))
      ),
      running = Nil
    )

    // One registered node with 4 CPUs and 2000 MB — enough to handle the queued task
    val registeredNodes = Seq(
      ResourceAvailable(cpu = 4, memory = 2000, scratch = 1000, gpu = Nil, image = None)
    )

    val result = decider.needNewNode(queueStat, registeredNodes, Seq.empty)

    result shouldBe empty
  }

  test(
    "should request new node when no existing nodes can handle queued tasks"
  ) {
    val queuedRequest = ResourceRequest((8, 8), 4000, 0, 0, None)
    val queueStat = QueueStat(
      queued = List(
        ("task1", VersionedResourceRequest(cv, queuedRequest))
      ),
      running = Nil
    )

    // One registered node with only 2 CPUs — not enough
    val registeredNodes = Seq(
      ResourceAvailable(cpu = 2, memory = 1000, scratch = 1000, gpu = Nil, image = None)
    )

    val result = decider.needNewNode(queueStat, registeredNodes, Seq.empty)

    result should not be empty
    result(queuedRequest) should equal(1)
  }

  test(
    "should not request new nodes when running jobs consume available resources but remaining can still handle queued tasks"
  ) {
    val queuedRequest = ResourceRequest((1, 1), 500, 0, 0, None)
    val runningAllocation =
      ResourceAllocated(cpu = 2, memory = 1000, scratch = 0, gpu = Nil, image = None)
    val queueStat = QueueStat(
      queued = List(
        ("task1", VersionedResourceRequest(cv, queuedRequest))
      ),
      running = List(
        ("task0", VersionedResourceAllocated(cv, runningAllocation))
      )
    )

    // Node has 4 CPUs total, running job uses 2 CPUs, so 2 CPUs remain — enough for the queued 1-CPU task
    val registeredNodes = Seq(
      ResourceAvailable(cpu = 4, memory = 2000, scratch = 1000, gpu = Nil, image = None)
    )

    val result = decider.needNewNode(queueStat, registeredNodes, Seq.empty)

    result shouldBe empty
  }

  test(
    "should request new node when running jobs exhaust available resources"
  ) {
    val queuedRequest = ResourceRequest((2, 2), 1000, 0, 0, None)
    val runningAllocation =
      ResourceAllocated(cpu = 4, memory = 2000, scratch = 0, gpu = Nil, image = None)
    val queueStat = QueueStat(
      queued = List(
        ("task1", VersionedResourceRequest(cv, queuedRequest))
      ),
      running = List(
        ("task0", VersionedResourceAllocated(cv, runningAllocation))
      )
    )

    // Node has 4 CPUs, running job uses all 4 — no capacity left
    val registeredNodes = Seq(
      ResourceAvailable(cpu = 4, memory = 2000, scratch = 1000, gpu = Nil, image = None)
    )

    val result = decider.needNewNode(queueStat, registeredNodes, Seq.empty)

    result should not be empty
  }

}
