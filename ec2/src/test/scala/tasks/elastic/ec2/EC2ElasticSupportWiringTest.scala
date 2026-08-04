package tasks.elastic.ec2

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global
import org.ekrich.config.ConfigFactory
import tasks.elastic.{CodeAddress, ConvertRunningToPending}
import tasks.shared.{
  CodeVersion,
  PendingJobId,
  ResourceAvailable,
  RunningJobId
}
import tasks.util.SimpleSocketAddress
import tasks.util.message.{LauncherName, Node}

class EC2ElasticSupportWiringTest extends AnyFunSuite with Matchers {

  private val ec2Config: EC2Config = {
    val overrides = ConfigFactory.parseString(
      """tasks.elastic.aws {
        |  subnetId = "subnet-abc"
        |  instanceTypes = ["m6i.large"]
        |  ami = "ami-x"
        |  tags = [ { key = "env", value = "test" } ]
        |}""".stripMargin
    )
    new EC2Config(
      overrides.withFallback(ConfigFactory.load("reference.conf")).resolve
    )
  }

  private def node(instanceId: String) = Node(
    RunningJobId(instanceId),
    ResourceAvailable(
      cpu = 1,
      memory = 100,
      scratch = 0,
      gpu = Nil,
      image = None
    ),
    LauncherName("launcher-1")
  )

  test(
    "the assembled support carries a spot aware ConvertRunningToPending, not the identity"
  ) {
    val ops = new FakeEC2Operations(
      spotRequestsByInstance =
        Map("i-1" -> FakeEC2Operations.spotRequest("sfr-1", Some("i-1")))
    )
    val support = EC2ElasticSupport.assemble(
      ec2Config,
      ops,
      Map.empty,
      Ref.unsafe[IO, Set[String]](Set.empty),
      None
    )

    val converted = support.convertRunningToPending
      .convertRunningToPending(RunningJobId("i-1"))
      .unsafeRunSync()

    converted shouldBe Some(PendingJobId("sfr-1"))
    converted should not be Some(PendingJobId("i-1"))
    ConvertRunningToPending.identity
      .convertRunningToPending(RunningJobId("i-1"))
      .unsafeRunSync() shouldBe Some(PendingJobId("i-1"))
  }

  test("the created node resolves pending ids through the same converter") {
    val ops = new FakeEC2Operations(
      spotRequestsByInstance =
        Map("i-1" -> FakeEC2Operations.spotRequest("sfr-1", Some("i-1")))
    )
    val support = EC2ElasticSupport.assemble(
      ec2Config,
      ops,
      Map.empty,
      Ref.unsafe[IO, Set[String]](Set.empty),
      None
    )

    val createNode = support.createNodeFactory(
      SimpleSocketAddress("master", 1234),
      "prefix",
      CodeAddress(SimpleSocketAddress("code", 8080), CodeVersion("v1"))
    )

    createNode
      .convertRunningToPending(RunningJobId("i-1"))
      .unsafeRunSync() shouldBe Some(PendingJobId("sfr-1"))
  }

  test("initializeNode tags the instance and registers it as a live worker") {
    val ops = new FakeEC2Operations()
    val registered = Ref.unsafe[IO, Set[String]](Set.empty)
    val support =
      EC2ElasticSupport.assemble(ec2Config, ops, Map.empty, registered, None)

    val createNode = support.createNodeFactory(
      SimpleSocketAddress("master", 1234),
      "prefix",
      CodeAddress(SimpleSocketAddress("code", 8080), CodeVersion("v1"))
    )

    createNode.initializeNode(node("i-1")).unsafeRunSync()

    ops.tagged.get.unsafeRunSync() shouldBe List(
      (List("i-1"), List("env" -> "test"))
    )
    registered.get.unsafeRunSync() shouldBe Set("i-1")
  }

  test(
    "a node which registered is protected from the pending node timeout shutdown"
  ) {
    val ops = new FakeEC2Operations(
      spotRequestsById =
        Map("sfr-1" -> FakeEC2Operations.spotRequest("sfr-1", Some("i-1")))
    )
    val registered = Ref.unsafe[IO, Set[String]](Set.empty)
    val support =
      EC2ElasticSupport.assemble(ec2Config, ops, Map.empty, registered, None)
    val createNode = support.createNodeFactory(
      SimpleSocketAddress("master", 1234),
      "prefix",
      CodeAddress(SimpleSocketAddress("code", 8080), CodeVersion("v1"))
    )

    createNode.initializeNode(node("i-1")).unsafeRunSync()
    support.shutdownFromNodeRegistry
      .shutdownPendingNode(PendingJobId("sfr-1"))
      .unsafeRunSync()

    ops.terminated.get.unsafeRunSync() shouldBe empty
  }
}
