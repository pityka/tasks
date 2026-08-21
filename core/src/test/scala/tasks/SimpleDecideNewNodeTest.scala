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
      ResourceAvailable(
        cpu = 4,
        memory = 2000,
        scratch = 1000,
        gpu = Nil,
        image = None
      )
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
      ResourceAvailable(
        cpu = 2,
        memory = 1000,
        scratch = 1000,
        gpu = Nil,
        image = None
      )
    )

    val result = decider.needNewNode(queueStat, registeredNodes, Seq.empty)

    result should not be empty
    result(queuedRequest) should equal(1)
  }

  test(
    "should not request new nodes when the capacity still free on a node can handle queued tasks"
  ) {
    val queuedRequest = ResourceRequest((1, 1), 500, 0, 0, None)
    val queueStat = QueueStat(
      queued = List(
        ("task1", VersionedResourceRequest(cv, queuedRequest))
      ),
      running = Nil
    )

    val freeOnRegisteredNodes = Seq(
      ResourceAvailable(
        cpu = 2,
        memory = 1000,
        scratch = 1000,
        gpu = Nil,
        image = None
      )
    )

    val result =
      decider.needNewNode(queueStat, freeOnRegisteredNodes, Seq.empty)

    result shouldBe empty
  }

  test(
    "should request new node when the registered nodes have no capacity left"
  ) {
    val queuedRequest = ResourceRequest((2, 2), 1000, 0, 0, None)
    val queueStat = QueueStat(
      queued = List(
        ("task1", VersionedResourceRequest(cv, queuedRequest))
      ),
      running = Nil
    )

    val freeOnRegisteredNodes = Seq(
      ResourceAvailable(
        cpu = 0,
        memory = 0,
        scratch = 1000,
        gpu = Nil,
        image = None
      )
    )

    val result =
      decider.needNewNode(queueStat, freeOnRegisteredNodes, Seq.empty)

    result should not be empty
  }

}
