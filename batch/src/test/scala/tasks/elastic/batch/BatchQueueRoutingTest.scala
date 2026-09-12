package tasks.elastic.batch

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import tasks.shared.ResourceAvailable

class BatchQueueRoutingTest extends AnyFunSuite with Matchers {

  private def cap(vcpus: Int, memoryMib: Int, gpus: Int = 0): InstanceCapacity =
    InstanceCapacity(vcpus, memoryMib, gpus)

  private def q(
      name: String,
      spot: Boolean,
      maxVcpus: Int,
      instances: List[InstanceCapacity]
  ): BatchQueueInfo = BatchQueueInfo(name, spot, maxVcpus, instances)

  private def req(cpu: Int, memory: Int, gpu: Int = 0): ResourceAvailable =
    ResourceAvailable(
      cpu = cpu,
      memory = memory,
      scratch = 0,
      gpu = (0 until gpu).toList,
      image = None
    )

  // canHostRequest

  test("canHostRequest: empty instances is treated as unknown-fit") {
    BatchCreateNode.canHostRequest(
      q("q", spot = false, maxVcpus = 100, instances = Nil),
      req(cpu = 4, memory = 1024)
    ) shouldBe true
  }

  test("canHostRequest: at least one instance fits") {
    val queue = q(
      "q",
      spot = false,
      maxVcpus = 100,
      instances = List(cap(2, 1024), cap(16, 32000))
    )
    BatchCreateNode.canHostRequest(
      queue,
      req(cpu = 8, memory = 8000)
    ) shouldBe true
  }

  test("canHostRequest: cpu too small for any instance") {
    val queue = q(
      "q",
      spot = false,
      maxVcpus = 100,
      instances = List(cap(2, 1024), cap(4, 32000))
    )
    BatchCreateNode.canHostRequest(
      queue,
      req(cpu = 8, memory = 1024)
    ) shouldBe false
  }

  test("canHostRequest: memory too small for any instance") {
    val queue = q(
      "q",
      spot = false,
      maxVcpus = 100,
      instances = List(cap(16, 1024), cap(16, 4096))
    )
    BatchCreateNode.canHostRequest(
      queue,
      req(cpu = 4, memory = 16000)
    ) shouldBe false
  }

  test("canHostRequest: gpus too few on any instance") {
    val queue = q(
      "q",
      spot = false,
      maxVcpus = 100,
      instances = List(cap(16, 32000, gpus = 1))
    )
    BatchCreateNode.canHostRequest(
      queue,
      req(cpu = 4, memory = 4000, gpu = 2)
    ) shouldBe false
  }

  test("canHostRequest: gpu request satisfied when instance has enough gpus") {
    val queue = q(
      "q",
      spot = false,
      maxVcpus = 100,
      instances = List(cap(16, 32000, gpus = 4))
    )
    BatchCreateNode.canHostRequest(
      queue,
      req(cpu = 4, memory = 4000, gpu = 2)
    ) shouldBe true
  }

  // largestInstanceVcpus

  test("largestInstanceVcpus: empty is MaxValue (sorts last on tight-fit)") {
    BatchCreateNode.largestInstanceVcpus(
      q("q", spot = false, maxVcpus = 100, instances = Nil)
    ) shouldBe Int.MaxValue
  }

  test("largestInstanceVcpus: returns the max vcpu across instances") {
    BatchCreateNode.largestInstanceVcpus(
      q(
        "q",
        spot = false,
        maxVcpus = 100,
        instances = List(cap(2, 1024), cap(16, 32000), cap(8, 8000))
      )
    ) shouldBe 16
  }

  // chooseQueue

  test("chooseQueue: no queues at all yields None") {
    BatchCreateNode.chooseQueue(
      queueInfos = Nil,
      request = req(cpu = 4, memory = 1024),
      onDemandHasRoom = true
    ) shouldBe None
  }

  test("chooseQueue: no queue fits yields None") {
    val queues = List(
      q("od", spot = false, maxVcpus = 100, instances = List(cap(2, 1024))),
      q("spot", spot = true, maxVcpus = 100, instances = List(cap(4, 1024)))
    )
    BatchCreateNode.chooseQueue(
      queues,
      req(cpu = 16, memory = 1024),
      onDemandHasRoom = true
    ) shouldBe None
  }

  test("chooseQueue: prefers on-demand when it has room") {
    val od =
      q("od", spot = false, maxVcpus = 100, instances = List(cap(16, 32000)))
    val spot =
      q("spot", spot = true, maxVcpus = 100, instances = List(cap(16, 32000)))
    BatchCreateNode.chooseQueue(
      List(spot, od),
      req(cpu = 4, memory = 1024),
      onDemandHasRoom = true
    ) shouldBe Some(od)
  }

  test("chooseQueue: falls back to spot when on-demand is full") {
    val od =
      q("od", spot = false, maxVcpus = 100, instances = List(cap(16, 32000)))
    val spot =
      q("spot", spot = true, maxVcpus = 100, instances = List(cap(16, 32000)))
    BatchCreateNode.chooseQueue(
      List(od, spot),
      req(cpu = 4, memory = 1024),
      onDemandHasRoom = false
    ) shouldBe Some(spot)
  }

  test(
    "chooseQueue: on-demand-only, no room, no spot -> fall back to on-demand"
  ) {
    val od =
      q("od", spot = false, maxVcpus = 100, instances = List(cap(16, 32000)))
    BatchCreateNode.chooseQueue(
      List(od),
      req(cpu = 4, memory = 1024),
      onDemandHasRoom = false
    ) shouldBe Some(od)
  }

  test("chooseQueue: picks tightest fit among on-demand candidates") {
    val small =
      q("small", spot = false, maxVcpus = 100, instances = List(cap(8, 8000)))
    val big =
      q("big", spot = false, maxVcpus = 100, instances = List(cap(64, 128000)))
    BatchCreateNode.chooseQueue(
      List(big, small),
      req(cpu = 4, memory = 1024),
      onDemandHasRoom = true
    ) shouldBe Some(small)
  }

  test(
    "chooseQueue: GPU request ignores on-demand/spot preference and tight-fits"
  ) {
    val cpuOd = q(
      "cpu-od",
      spot = false,
      maxVcpus = 100,
      instances = List(cap(16, 32000, gpus = 0))
    )
    val gpuSpotSmall = q(
      "gpu-spot-small",
      spot = true,
      maxVcpus = 100,
      instances = List(cap(8, 61000, gpus = 1))
    )
    val gpuOdBig = q(
      "gpu-od-big",
      spot = false,
      maxVcpus = 100,
      instances = List(cap(64, 488000, gpus = 8))
    )
    BatchCreateNode.chooseQueue(
      List(cpuOd, gpuSpotSmall, gpuOdBig),
      req(cpu = 4, memory = 4000, gpu = 1),
      onDemandHasRoom = true
    ) shouldBe Some(gpuSpotSmall)
  }

  test("chooseQueue: unknown-fit queues sort last") {
    val known =
      q("known", spot = false, maxVcpus = 100, instances = List(cap(16, 32000)))
    val unknown = q("unknown", spot = false, maxVcpus = 100, instances = Nil)
    BatchCreateNode.chooseQueue(
      List(unknown, known),
      req(cpu = 4, memory = 1024),
      onDemandHasRoom = true
    ) shouldBe Some(known)
  }

  test("chooseQueue: unknown-fit queue chosen when nothing else fits") {
    val tinyKnown =
      q("tiny", spot = false, maxVcpus = 100, instances = List(cap(2, 1024)))
    val unknown = q("unknown", spot = false, maxVcpus = 100, instances = Nil)
    BatchCreateNode.chooseQueue(
      List(tinyKnown, unknown),
      req(cpu = 16, memory = 1024),
      onDemandHasRoom = true
    ) shouldBe Some(unknown)
  }
}
