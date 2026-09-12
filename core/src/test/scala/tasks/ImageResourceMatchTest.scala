package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import tasks.elastic.SimpleDecideNewNode
import tasks.shared._
import tasks.util.message.QueueStat

class ImageResourceMatchTest extends FunSuite with Matchers {

  test(
    "canFulfillRequest(ResourceAllocated) allows any node when allocated image is None"
  ) {
    val node = ResourceAvailable(
      cpu = 4,
      memory = 2000,
      scratch = 1000,
      gpu = Nil,
      image = Some("A")
    )
    val allocated = ResourceAllocated(
      cpu = 2,
      memory = 1000,
      scratch = 0,
      gpu = Nil,
      image = None
    )
    node.canFulfillRequest(allocated) shouldBe true
  }

  test("canFulfillRequest(ResourceAllocated) matches when images agree") {
    val node = ResourceAvailable(
      cpu = 4,
      memory = 2000,
      scratch = 1000,
      gpu = Nil,
      image = Some("A")
    )
    val allocated = ResourceAllocated(
      cpu = 2,
      memory = 1000,
      scratch = 0,
      gpu = Nil,
      image = Some("A")
    )
    node.canFulfillRequest(allocated) shouldBe true
  }

  test(
    "canFulfillRequest(ResourceAllocated) rejects when allocated demands an image the node lacks"
  ) {
    val node = ResourceAvailable(
      cpu = 4,
      memory = 2000,
      scratch = 1000,
      gpu = Nil,
      image = Some("A")
    )
    val allocated = ResourceAllocated(
      cpu = 2,
      memory = 1000,
      scratch = 0,
      gpu = Nil,
      image = Some("B")
    )
    node.canFulfillRequest(allocated) shouldBe false
  }

  test(
    "canFulfillRequest(ResourceAllocated) rejects when allocated demands an image and the node has none"
  ) {
    val node = ResourceAvailable(
      cpu = 4,
      memory = 2000,
      scratch = 1000,
      gpu = Nil,
      image = None
    )
    val allocated = ResourceAllocated(
      cpu = 2,
      memory = 1000,
      scratch = 0,
      gpu = Nil,
      image = Some("A")
    )
    node.canFulfillRequest(allocated) shouldBe false
  }

  test(
    "SimpleDecideNewNode does not deduct a running image=A job from a registered image=B node"
  ) {
    // With two nodes carrying different images, a running job scheduled on the
    // image=A node must not silently subtract capacity from the image=B node.
    // If it does, the subsequent image=B request would appear fulfilled and no
    // new node would be requested even though the image-B node is idle and the
    // image-A node is full.
    val cv = CodeVersion("test")
    implicit val config: tasks.util.config.TasksConfig =
      tasks.util.config.parse(() => org.ekrich.config.ConfigFactory.load())
    val decider = new SimpleDecideNewNode(cv)

    val queuedRequest =
      ResourceRequest((2, 2), 1000, 0, 0, image = Some("B"))
    val runningAllocation =
      ResourceAllocated(
        cpu = 2,
        memory = 1000,
        scratch = 0,
        gpu = Nil,
        image = Some("A")
      )

    val queueStat = QueueStat(
      queued = List(("task-b", VersionedResourceRequest(cv, queuedRequest))),
      running =
        List(("task-a", VersionedResourceAllocated(cv, runningAllocation)))
    )

    // Node A is fully consumed by the running job. Node B is idle but has the
    // wrong image for a request without an image constraint; here the queued
    // request explicitly wants image=B so it must schedule on node B.
    val registeredNodes = Seq(
      ResourceAvailable(
        cpu = 2,
        memory = 1000,
        scratch = 1000,
        gpu = Nil,
        image = Some("A")
      ),
      ResourceAvailable(
        cpu = 2,
        memory = 1000,
        scratch = 1000,
        gpu = Nil,
        image = Some("B")
      )
    )

    val result = decider.needNewNode(queueStat, registeredNodes, Seq.empty)

    // After the fix, node B has enough free capacity for the image=B request,
    // so no new node needs to be spawned. Before the fix, the image=A running
    // job would have been subtracted from node B (because
    // canFulfillRequest(ResourceAllocated) ignored image), leaving no capacity
    // for the queued request and forcing an unnecessary spawn.
    result shouldBe empty
  }

}
