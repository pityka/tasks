package tasks.elastic.ecs

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.ekrich.config.ConfigFactory
import tasks.shared.ResourceRequest

class EcsConfigTest extends AnyFunSuite with Matchers {

  private def raw(extra: String) = {
    val hocon =
      s"""
         |tasks.elastic.ecs {
         |  region = "us-east-1"
         |  cluster = "workers"
         |  capacityProvider = "workers-managed-instances"
         |  capacityProviderBase = 0
         |  capacityProviderWeight = 1
         |  containerName = "worker"
         |  taskDefinition = "default-td"
         |  minimumCpu = 1
         |  minimumMemory = 512
         |  startedBy = "tasks-elastic"
         |  stopReason = "stopped"
         |  environment = []
         |  tags = []
         |  $extra
         |}
         |""".stripMargin
    ConfigFactory.parseString(hocon)
  }

  private def cfg(extra: String): EcsConfig = new EcsConfig(raw(extra))

  test("a non-empty cluster and capacityProvider are accepted") {
    val c = cfg("")
    c.cluster shouldBe "workers"
    c.capacityProvider shouldBe "workers-managed-instances"
  }

  test("an empty cluster is rejected") {
    an[IllegalArgumentException] should be thrownBy cfg("""cluster = """"")
  }

  test("an empty capacityProvider is rejected") {
    an[IllegalArgumentException] should be thrownBy cfg(
      """capacityProvider = """""
    )
  }

  test("resolveTaskDefinition(None) returns the default taskDefinition") {
    cfg("").resolveTaskDefinition(None) shouldBe Right("default-td")
  }

  test("resolveTaskDefinition(Some(x)) returns the mapped task definition") {
    val c = cfg(
      """
        |taskDefinitionsByImage = [
        |  { image = "my-image:v1", taskDefinition = "my-td-v1" },
        |  { image = "my-image:v2", taskDefinition = "my-td-v2" }
        |]
        |""".stripMargin
    )
    c.resolveTaskDefinition(Some("my-image:v1")) shouldBe Right("my-td-v1")
    c.resolveTaskDefinition(Some("my-image:v2")) shouldBe Right("my-td-v2")
  }

  test("resolveTaskDefinition(Some(x)) fails fast when image is not mapped") {
    val c = cfg(
      """
        |taskDefinitionsByImage = [
        |  { image = "my-image:v1", taskDefinition = "my-td-v1" }
        |]
        |""".stripMargin
    )
    val result = c.resolveTaskDefinition(Some("unknown:tag"))
    result.isLeft shouldBe true
    val err = result.left.getOrElse(fail("expected Left"))
    err should include("unknown:tag")
    err should include("tasks.elastic.ecs.taskDefinitionsByImage")
  }

  test("resolveRegion returns the configured region when it is set") {
    EcsConfig.resolveRegion("eu-west-1") shouldBe "eu-west-1"
  }

  test("a startedBy longer than 36 characters is rejected") {
    an[IllegalArgumentException] should be thrownBy cfg(
      s"""startedBy = "${"x" * 37}""""
    )
  }

  test("an odd number of tag entries is rejected") {
    an[IllegalArgumentException] should be thrownBy cfg(
      """tags = ["only-a-key"]"""
    )
  }

  test("environment parses as alternating key value pairs") {
    cfg(
      """environment = ["OTEL_ENDPOINT", "http://x", "FOO", "bar"]"""
    ).extraEnvironment shouldBe Map(
      "OTEL_ENDPOINT" -> "http://x",
      "FOO" -> "bar"
    )
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
