package tasks.elastic.ecs

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import tasks.shared.ResourceRequest

class EcsConfigTest extends AnyFunSuite with Matchers {

  private val minimal =
    EcsConfig(
      cluster = "workers",
      capacityProvider = "workers-managed-instances",
      containerName = "worker",
      taskDefinition = "default-td"
    )

  test("a non-empty cluster and capacityProvider are accepted") {
    minimal.cluster shouldBe "workers"
    minimal.capacityProvider shouldBe "workers-managed-instances"
  }

  test("the short constructor fills in the defaults") {
    minimal.region shouldBe None
    minimal.capacityProviderBase shouldBe 0
    minimal.capacityProviderWeight shouldBe 1
    minimal.minimumCpu shouldBe 1
    minimal.minimumMemory shouldBe 512
    minimal.startedBy shouldBe "tasks-elastic"
    minimal.taskDefinitionsByImage shouldBe empty
    minimal.extraEnvironment shouldBe empty
    minimal.tags shouldBe empty
  }

  test("an empty cluster is rejected") {
    an[IllegalArgumentException] should be thrownBy minimal.copy(cluster = "")
  }

  test("an empty capacityProvider is rejected") {
    an[IllegalArgumentException] should be thrownBy minimal.copy(
      capacityProvider = ""
    )
  }

  test("an empty containerName is rejected") {
    an[IllegalArgumentException] should be thrownBy minimal.copy(
      containerName = ""
    )
  }

  test("an empty taskDefinition is rejected") {
    an[IllegalArgumentException] should be thrownBy minimal.copy(
      taskDefinition = ""
    )
  }

  test("resolveTaskDefinition(None) returns the default taskDefinition") {
    minimal.resolveTaskDefinition(None) shouldBe Right("default-td")
  }

  test("resolveTaskDefinition(Some(x)) returns the mapped task definition") {
    val c = minimal
      .withTaskDefinitionForImage("my-image:v1", "my-td-v1")
      .withTaskDefinitionForImage("my-image:v2", "my-td-v2")
    c.resolveTaskDefinition(Some("my-image:v1")) shouldBe Right("my-td-v1")
    c.resolveTaskDefinition(Some("my-image:v2")) shouldBe Right("my-td-v2")
    c.resolveTaskDefinition(None) shouldBe Right("default-td")
  }

  test("resolveTaskDefinition(Some(x)) fails fast when image is not mapped") {
    val c = minimal.withTaskDefinitionForImage("my-image:v1", "my-td-v1")
    val result = c.resolveTaskDefinition(Some("unknown:tag"))
    result.isLeft shouldBe true
    val err = result.left.getOrElse(fail("expected Left"))
    err should include("unknown:tag")
    err should include("withTaskDefinitionForImage")
  }

  test("resolveTaskDefinition(Some(x)) fails when no image is mapped") {
    minimal.resolveTaskDefinition(Some("my-image:v1")).isLeft shouldBe true
  }

  test("resolveRegion returns the configured region when it is set") {
    EcsConfig.resolveRegion(Some("eu-west-1")) shouldBe "eu-west-1"
  }

  test("a startedBy longer than 36 characters is rejected") {
    an[IllegalArgumentException] should be thrownBy minimal.withStartedBy(
      "x" * 37
    )
  }

  test("withEnvironment accumulates entries") {
    minimal
      .withEnvironment("OTEL_ENDPOINT" -> "http://x")
      .withEnvironment("FOO" -> "bar")
      .extraEnvironment shouldBe Map(
      "OTEL_ENDPOINT" -> "http://x",
      "FOO" -> "bar"
    )
  }

  test("withTags accumulates entries") {
    minimal
      .withTags("team" -> "platform", "env" -> "prod")
      .tags shouldBe Map("team" -> "platform", "env" -> "prod")
  }

  test("toString does not leak environment values") {
    minimal
      .withEnvironment("SECRET" -> "hunter2")
      .toString should not include ("hunter2")
  }
}

class EcsPlacementTest extends AnyFunSuite with Matchers {

  test("one vCPU is 1024 ECS CPU units") {
    EcsOperations.vcpuToCpuUnits(1) shouldBe 1024
    EcsOperations.vcpuToCpuUnits(8) shouldBe 8192
    EcsOperations.cpuUnitsToVcpu(4096) shouldBe 4
  }

  test("RESOURCE and AGENT failures are capacity shortages") {
    TaskPlacementFailure("RESOURCE:CPU", None).isCapacityShortage shouldBe true
    TaskPlacementFailure("RESOURCE:GPU", None).isCapacityShortage shouldBe true
    TaskPlacementFailure("AGENT", None).isCapacityShortage shouldBe true
  }

  test("ATTRIBUTE failures are not capacity shortages") {
    TaskPlacementFailure("ATTRIBUTE", None).isCapacityShortage shouldBe false
  }

  test("selectResources clamps up to the configured minimums") {
    val request = ResourceRequest(cpu = 1, memory = 128, scratch = 0, gpu = 0)
    val selected = EcsCreateNode.selectResources(request, 4, 2048)
    selected.cpu shouldBe 4
    selected.memory shouldBe 2048
  }

  test("selectResources keeps a request larger than the minimums") {
    val request =
      ResourceRequest(cpu = 16, memory = 8192, scratch = 10, gpu = 0)
    val selected = EcsCreateNode.selectResources(request, 4, 2048)
    selected.cpu shouldBe 16
    selected.memory shouldBe 8192
    selected.scratch shouldBe 10
  }

  test("selectResources expands the gpu count into a device list") {
    val request = ResourceRequest(cpu = 1, memory = 128, scratch = 0, gpu = 4)
    EcsCreateNode
      .selectResources(request, 1, 128)
      .gpu shouldBe List(0, 1, 2, 3)
  }

  test("renderCapacity reports an empty pool explicitly") {
    EcsCreateNode.renderCapacity(Nil) should include(
      "no ACTIVE container instances"
    )
  }

  test("renderCapacity reports remaining capacity in vCPUs") {
    val capacity = List(
      ContainerInstanceCapacity(
        arn = "arn:aws:ecs:us-east-1:1:container-instance/workers/abc123",
        agentConnected = true,
        remainingCpuUnits = 4096,
        remainingMemoryMib = 16000,
        remainingGpus = 2
      )
    )
    val rendered = EcsCreateNode.renderCapacity(capacity)
    rendered should include("abc123")
    rendered should include("vcpu=4")
    rendered should include("gpu=2")
  }
}
