package tasks.elastic.ecs

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import tasks.shared.ResourceAvailable

class EcsCapacityRoutingTest extends AnyFunSuite with Matchers {

  private def shape(
      vcpus: Int,
      memoryMib: Int,
      gpus: Int
  ): InstanceTypeCapacity = InstanceTypeCapacity(vcpus, memoryMib, gpus)

  private def provider(
      name: String,
      instanceTypes: List[InstanceTypeCapacity],
      canScaleOut: Boolean
  ): CapacityProviderInfo =
    CapacityProviderInfo(name, instanceTypes, canScaleOut)

  private def req(cpu: Int, memory: Int, gpu: Int): ResourceAvailable =
    ResourceAvailable(
      cpu = cpu,
      memory = memory,
      scratch = 0,
      gpu = (0 until gpu).toList,
      image = None
    )

  private def registered(
      vcpus: Int,
      memoryMib: Int,
      gpus: Int
  ): ContainerInstanceCapacity =
    ContainerInstanceCapacity(
      arn = "arn:aws:ecs:us-east-1:1:container-instance/workers/abc123",
      agentConnected = true,
      remainingCpuUnits = EcsOperations.vcpuToCpuUnits(vcpus),
      remainingMemoryMib = memoryMib,
      remainingGpus = gpus
    )

  test(
    "canHostRequest: undiscovered instance types are treated as unknown-fit"
  ) {
    EcsCreateNode.canHostRequest(
      provider("cp", Nil, canScaleOut = true),
      req(cpu = 64, memory = 256000, gpu = 8)
    ) shouldBe true
  }

  test("canHostRequest: at least one instance type fits") {
    EcsCreateNode.canHostRequest(
      provider("cp", List(shape(2, 4096, 0), shape(16, 32000, 0)), true),
      req(cpu = 8, memory = 16000, gpu = 0)
    ) shouldBe true
  }

  test("canHostRequest: cpu too small for any instance type") {
    EcsCreateNode.canHostRequest(
      provider("cp", List(shape(2, 32000, 0), shape(4, 32000, 0)), true),
      req(cpu = 8, memory = 1024, gpu = 0)
    ) shouldBe false
  }

  test("canHostRequest: memory too small for any instance type") {
    EcsCreateNode.canHostRequest(
      provider("cp", List(shape(16, 4096, 0), shape(16, 8192, 0)), true),
      req(cpu = 4, memory = 32000, gpu = 0)
    ) shouldBe false
  }

  test("canHostRequest: gpus too few on any instance type") {
    EcsCreateNode.canHostRequest(
      provider("cp", List(shape(16, 32000, 1)), true),
      req(cpu = 4, memory = 4096, gpu = 2)
    ) shouldBe false
  }

  test("canHostRequest: gpu request met by a gpu instance type") {
    EcsCreateNode.canHostRequest(
      provider("gpu-cp", List(shape(16, 32000, 4)), true),
      req(cpu = 4, memory = 4096, gpu = 2)
    ) shouldBe true
  }

  test("hasRoomFor: no container instance means no room") {
    EcsCreateNode.hasRoomFor(Nil, req(cpu = 1, memory = 512, gpu = 0)) shouldBe
      false
  }

  test("hasRoomFor: an instance with enough remaining capacity has room") {
    EcsCreateNode.hasRoomFor(
      List(registered(2, 4096, 0), registered(8, 32000, 0)),
      req(cpu = 4, memory = 16000, gpu = 0)
    ) shouldBe true
  }

  test("hasRoomFor: remaining cpu units are compared in whole vCPUs") {
    val halfAVcpuShort = ContainerInstanceCapacity(
      arn = "arn:aws:ecs:us-east-1:1:container-instance/workers/abc123",
      agentConnected = true,
      remainingCpuUnits = 3584,
      remainingMemoryMib = 32000,
      remainingGpus = 0
    )
    EcsCreateNode.hasRoomFor(
      List(halfAVcpuShort),
      req(cpu = 4, memory = 1024, gpu = 0)
    ) shouldBe false
    EcsCreateNode.hasRoomFor(
      List(halfAVcpuShort),
      req(cpu = 3, memory = 1024, gpu = 0)
    ) shouldBe true
  }

  test("hasRoomFor: remaining memory is respected") {
    EcsCreateNode.hasRoomFor(
      List(registered(16, 1024, 0)),
      req(cpu = 4, memory = 16000, gpu = 0)
    ) shouldBe false
  }

  test("hasRoomFor: remaining gpus are respected") {
    EcsCreateNode.hasRoomFor(
      List(registered(16, 32000, 1)),
      req(cpu = 4, memory = 4096, gpu = 2)
    ) shouldBe false
  }

  test("autoScalingGroupName reads the name out of an asg arn") {
    EcsCapacityDiscovery.autoScalingGroupName(
      "arn:aws:autoscaling:us-east-1:1:autoScalingGroup:uuid:autoScalingGroupName/ecs-workers-spot"
    ) shouldBe Some("ecs-workers-spot")
  }

  test("autoScalingGroupName rejects an arn without a name") {
    EcsCapacityDiscovery.autoScalingGroupName("not-an-arn") shouldBe None
    EcsCapacityDiscovery.autoScalingGroupName("") shouldBe None
    EcsCapacityDiscovery.autoScalingGroupName(
      "arn:aws:autoscaling:us-east-1:1:autoScalingGroup:uuid:autoScalingGroupName/"
    ) shouldBe None
  }

  private def asg(
      instanceCount: Int,
      desiredCapacity: Int,
      maxSize: Int
  ): EcsCapacityDiscovery.AutoScalingGroupDetails =
    EcsCapacityDiscovery.AutoScalingGroupDetails(
      instanceTypes = List("m6i.large"),
      launchTemplate = None,
      instanceCount = instanceCount,
      desiredCapacity = desiredCapacity,
      maxSize = maxSize
    )

  test("an auto scaling group at its maximum cannot scale out") {
    asg(
      instanceCount = 4,
      desiredCapacity = 4,
      maxSize = 4
    ).canScaleOut shouldBe
      false
  }

  test("an auto scaling group below its maximum can scale out") {
    asg(
      instanceCount = 2,
      desiredCapacity = 2,
      maxSize = 4
    ).canScaleOut shouldBe
      true
  }

  test("an auto scaling group still launching instances can scale out") {
    asg(
      instanceCount = 2,
      desiredCapacity = 4,
      maxSize = 4
    ).canScaleOut shouldBe
      true
  }

  test("an auto scaling group with a maximum of zero cannot scale out") {
    asg(
      instanceCount = 0,
      desiredCapacity = 0,
      maxSize = 0
    ).canScaleOut shouldBe
      false
  }

  test("renderInstanceTypes reports undiscovered shapes as unknown") {
    EcsCapacityDiscovery.renderInstanceTypes(Nil) shouldBe "unknown"
  }

  test("renderInstanceTypes reports every discovered shape") {
    val rendered = EcsCapacityDiscovery.renderInstanceTypes(
      List(shape(2, 4096, 0), shape(16, 32000, 4))
    )
    rendered should include("vcpu=2")
    rendered should include("memMiB=32000")
    rendered should include("gpu=4")
  }
}
