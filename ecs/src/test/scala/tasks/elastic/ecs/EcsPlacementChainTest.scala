package tasks.elastic.ecs

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.ekrich.config.ConfigFactory

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global

import tasks.elastic._
import tasks.shared._
import tasks.util.SimpleSocketAddress
import tasks.util.config.TasksConfig

object EcsPlacementChainTest {

  private val storageURI = {
    val tmp = java.io.File.createTempFile("tasks-ecs-test", ".temp")
    tmp.delete()
    tmp.getAbsolutePath
  }

  implicit val tasksConfig: TasksConfig =
    tasks.util.config.parse(() =>
      tasks.util.loadConfig(
        Some(
          ConfigFactory.parseString(
            s"tasks.fileservice.storageURI=$storageURI"
          )
        )
      )
    )

  val shortage: Either[List[TaskPlacementFailure], String] =
    Left(List(TaskPlacementFailure("RESOURCE:MEMORY", None)))

  val rejected: Either[List[TaskPlacementFailure], String] =
    Left(List(TaskPlacementFailure("ATTRIBUTE", None)))

  def placed(arn: String): Either[List[TaskPlacementFailure], String] =
    Right(arn)

  def anyShape(name: String): CapacityProviderInfo =
    CapacityProviderInfo(
      name,
      List(InstanceTypeCapacity(64, 512000, 8)),
      canScaleOut = true
    )

  def shaped(
      name: String,
      vcpus: Int,
      memoryMib: Int
  ): CapacityProviderInfo =
    CapacityProviderInfo(
      name,
      List(InstanceTypeCapacity(vcpus, memoryMib, 0)),
      canScaleOut = true
    )

  def maxedOut(name: String): CapacityProviderInfo =
    CapacityProviderInfo(
      name,
      List(InstanceTypeCapacity(64, 512000, 8)),
      canScaleOut = false
    )

  def free(vcpus: Int, memoryMib: Int): ContainerInstanceCapacity =
    ContainerInstanceCapacity(
      arn = "arn:aws:ecs:us-east-1:1:container-instance/workers/abc123",
      agentConnected = true,
      remainingCpuUnits = EcsOperations.vcpuToCpuUnits(vcpus),
      remainingMemoryMib = memoryMib,
      remainingGpus = 0
    )

  final class FakeEcsOperations(
      calls: Ref[IO, List[String]],
      info: String => IO[CapacityProviderInfo],
      remaining: PlacementTarget => IO[List[ContainerInstanceCapacity]],
      run: PlacementTarget => IO[Either[List[TaskPlacementFailure], String]]
  ) extends EcsOperations {

    def runTask(
        spec: WorkerTaskSpec,
        target: PlacementTarget,
        clientToken: String
    ): IO[Either[List[TaskPlacementFailure], String]] =
      calls.update(_ :+ s"runTask:$target:$clientToken") *> run(target)

    def stopTask(taskArn: String, reason: String): IO[Unit] =
      calls.update(_ :+ s"stopTask:$taskArn")

    def placeableCapacity(
        target: PlacementTarget
    ): IO[List[ContainerInstanceCapacity]] =
      calls.update(_ :+ s"placeableCapacity:$target") *> remaining(target)

    def capacityProviderInfo(
        capacityProvider: String
    ): IO[CapacityProviderInfo] =
      calls.update(_ :+ s"capacityProviderInfo:$capacityProvider") *>
        info(capacityProvider)
  }

  def createNode(
      ops: EcsOperations,
      capacityProviders: List[String]
  ): EcsCreateNode =
    new EcsCreateNode(
      masterAddress = SimpleSocketAddress("master", 1234),
      masterPrefix = "prefix",
      codeAddress =
        CodeAddress(SimpleSocketAddress("master", 1235), CodeVersion("1")),
      ops = ops,
      ecsConfig =
        EcsConfig("workers", capacityProviders, "worker", "worker-td"),
      resolvedRegion = "us-east-1"
    )

  def request(cpu: Int, memory: Int): ResourceRequest =
    ResourceRequest(cpu = cpu, memory = memory, scratch = 0, gpu = 0)

  def runTaskTargets(calls: List[String]): List[String] =
    calls.collect {
      case call if call.startsWith("runTask:") =>
        call.split(':').drop(1).dropRight(1).mkString(":")
    }

  def clientTokens(calls: List[String]): List[String] =
    calls.collect {
      case call if call.startsWith("runTask:") => call.split(':').last
    }
}

class EcsPlacementChainTest extends AnyFunSuite with Matchers {
  import EcsPlacementChainTest._

  private def fixture(
      capacityProviders: List[String],
      info: String => IO[CapacityProviderInfo],
      remaining: PlacementTarget => IO[List[ContainerInstanceCapacity]],
      run: PlacementTarget => IO[Either[List[TaskPlacementFailure], String]]
  ): (Ref[IO, List[String]], EcsCreateNode) = {
    val calls = Ref.unsafe[IO, List[String]](Nil)
    val ops = new FakeEcsOperations(calls, info, remaining, run)
    (calls, createNode(ops, capacityProviders))
  }

  private def place(
      node: EcsCreateNode,
      requestSize: ResourceRequest
  ): Either[String, (PendingJobId, ResourceAvailable)] =
    node.requestOneNewJobFromJobScheduler(requestSize).unsafeRunSync()

  private def noRoom: PlacementTarget => IO[List[ContainerInstanceCapacity]] =
    _ => IO.pure(Nil)

  test("external instances are offered the request before any provider") {
    val (calls, node) = fixture(
      capacityProviders = List("spot", "on-demand"),
      info = name => IO.pure(anyShape(name)),
      remaining = noRoom,
      run = {
        case PlacementTarget.External =>
          IO.pure(placed("task/workers/external"))
        case _ => IO.pure(shortage)
      }
    )

    place(node, request(cpu = 4, memory = 8000)).map(_._1) shouldBe
      Right(PendingJobId("task/workers/external"))
    runTaskTargets(calls.get.unsafeRunSync()) shouldBe List("External")
  }

  test("a provider which reports a failure hands the request to the next") {
    val (calls, node) = fixture(
      capacityProviders = List("spot", "on-demand"),
      info = name => IO.pure(anyShape(name)),
      remaining = noRoom,
      run = {
        case PlacementTarget.CapacityProvider("on-demand") =>
          IO.pure(placed("task/workers/ondemand"))
        case _ => IO.pure(shortage)
      }
    )

    place(node, request(cpu = 4, memory = 8000)).map(_._1) shouldBe
      Right(PendingJobId("task/workers/ondemand"))
    runTaskTargets(calls.get.unsafeRunSync()) shouldBe List(
      "External",
      "capacity-provider:spot",
      "capacity-provider:on-demand"
    )
  }

  test("providers are walked in the configured order") {
    val (calls, node) = fixture(
      capacityProviders = List("third", "first", "second"),
      info = name => IO.pure(anyShape(name)),
      remaining = noRoom,
      run = {
        case PlacementTarget.CapacityProvider("second") =>
          IO.pure(placed("task/workers/second"))
        case _ => IO.pure(shortage)
      }
    )

    place(node, request(cpu = 4, memory = 8000)).map(_._1) shouldBe
      Right(PendingJobId("task/workers/second"))
    runTaskTargets(calls.get.unsafeRunSync()) shouldBe List(
      "External",
      "capacity-provider:third",
      "capacity-provider:first",
      "capacity-provider:second"
    )
  }

  test("each placement attempt carries its own client token") {
    val (calls, node) = fixture(
      capacityProviders = List("spot", "on-demand"),
      info = name => IO.pure(anyShape(name)),
      remaining = noRoom,
      run = {
        case PlacementTarget.CapacityProvider("on-demand") =>
          IO.pure(placed("task/workers/ondemand"))
        case _ => IO.pure(shortage)
      }
    )

    place(node, request(cpu = 4, memory = 8000))
    val tokens = clientTokens(calls.get.unsafeRunSync())
    tokens should have size 3
    tokens.distinct should have size 3
    tokens.foreach(_ should not be empty)
  }

  test("a provider whose instance types are too small is never asked") {
    val (calls, node) = fixture(
      capacityProviders = List("small", "big"),
      info = {
        case "small" => IO.pure(shaped("small", vcpus = 4, memoryMib = 16000))
        case name    => IO.pure(anyShape(name))
      },
      remaining = noRoom,
      run = {
        case PlacementTarget.CapacityProvider("big") =>
          IO.pure(placed("task/workers/big"))
        case _ => IO.pure(shortage)
      }
    )

    place(node, request(cpu = 16, memory = 64000)).map(_._1) shouldBe
      Right(PendingJobId("task/workers/big"))
    runTaskTargets(calls.get.unsafeRunSync()) shouldBe List(
      "External",
      "capacity-provider:big"
    )
  }

  test("a provider at its maximum with no room is never asked") {
    val (calls, node) = fixture(
      capacityProviders = List("full", "overflow"),
      info = {
        case "full" => IO.pure(maxedOut("full"))
        case name   => IO.pure(anyShape(name))
      },
      remaining = noRoom,
      run = {
        case PlacementTarget.CapacityProvider("overflow") =>
          IO.pure(placed("task/workers/overflow"))
        case _ => IO.pure(shortage)
      }
    )

    place(node, request(cpu = 4, memory = 8000)).map(_._1) shouldBe
      Right(PendingJobId("task/workers/overflow"))
    val recorded = calls.get.unsafeRunSync()
    runTaskTargets(recorded) shouldBe List(
      "External",
      "capacity-provider:overflow"
    )
    recorded should contain("placeableCapacity:capacity-provider:full")
  }

  test("a provider at its maximum with room is still asked") {
    val (calls, node) = fixture(
      capacityProviders = List("full", "overflow"),
      info = {
        case "full" => IO.pure(maxedOut("full"))
        case name   => IO.pure(anyShape(name))
      },
      remaining = {
        case PlacementTarget.CapacityProvider("full") =>
          IO.pure(List(free(vcpus = 16, memoryMib = 64000)))
        case _ => IO.pure(Nil)
      },
      run = {
        case PlacementTarget.CapacityProvider("full") =>
          IO.pure(placed("task/workers/full"))
        case _ => IO.pure(shortage)
      }
    )

    place(node, request(cpu = 4, memory = 8000)).map(_._1) shouldBe
      Right(PendingJobId("task/workers/full"))
    runTaskTargets(calls.get.unsafeRunSync()) shouldBe List(
      "External",
      "capacity-provider:full"
    )
  }

  test("an exhausted chain reports the last failure and every skip") {
    val (calls, node) = fixture(
      capacityProviders = List("small", "tiny"),
      info = {
        case "small" => IO.pure(shaped("small", vcpus = 4, memoryMib = 16000))
        case name    => IO.pure(shaped(name, vcpus = 2, memoryMib = 8000))
      },
      remaining = noRoom,
      run = _ => IO.pure(shortage)
    )

    val result = place(node, request(cpu = 16, memory = 64000))
    result.isLeft shouldBe true
    val message = result.left.getOrElse(fail("expected a Left"))
    message should include("External")
    message should include("RESOURCE:MEMORY")
    message should include("Skipped")
    message should include("small")
    message should include("tiny")
    runTaskTargets(calls.get.unsafeRunSync()) shouldBe List("External")
  }

  test("an exhausted chain without skips names the last target tried") {
    val (_, node) = fixture(
      capacityProviders = List("spot", "on-demand"),
      info = name => IO.pure(anyShape(name)),
      remaining = noRoom,
      run = _ => IO.pure(shortage)
    )

    val result = place(node, request(cpu = 4, memory = 8000))
    val message = result.left.getOrElse(fail("expected a Left"))
    message should include("capacity-provider:on-demand")
    message should not include ("Skipped")
  }

  test("a non-capacity failure is reported without a capacity dump") {
    val (calls, node) = fixture(
      capacityProviders = Nil,
      info = name => IO.pure(anyShape(name)),
      remaining = noRoom,
      run = _ => IO.pure(rejected)
    )

    val result = place(node, request(cpu = 4, memory = 8000))
    val message = result.left.getOrElse(fail("expected a Left"))
    message should include("ATTRIBUTE")
    calls.get
      .unsafeRunSync()
      .filter(
        _.startsWith("placeableCapacity")
      ) shouldBe empty
  }

  test("a failed discovery places no task and names the permissions") {
    val (calls, node) = fixture(
      capacityProviders = List("spot", "on-demand"),
      info = _ =>
        IO.raiseError(new RuntimeException("AccessDeniedException from IAM")),
      remaining = noRoom,
      run = _ => IO.pure(placed("task/workers/never"))
    )

    val result = place(node, request(cpu = 4, memory = 8000))
    result.isLeft shouldBe true
    val message = result.left.getOrElse(fail("expected a Left"))
    message should include("AccessDeniedException from IAM")
    message should include("DescribeCapacityProviders")
    message should include("DescribeAutoScalingGroups")
    message should include("DescribeInstanceTypes")
    runTaskTargets(calls.get.unsafeRunSync()) shouldBe empty
  }

  test("without capacity providers nothing is discovered") {
    val (calls, node) = fixture(
      capacityProviders = Nil,
      info = _ => IO.raiseError(new RuntimeException("must not be called")),
      remaining = noRoom,
      run = {
        case PlacementTarget.External =>
          IO.pure(placed("task/workers/external"))
        case _ => IO.pure(shortage)
      }
    )

    place(node, request(cpu = 4, memory = 8000)).map(_._1) shouldBe
      Right(PendingJobId("task/workers/external"))
    val recorded = calls.get.unsafeRunSync()
    runTaskTargets(recorded) shouldBe List("External")
    recorded.filter(_.startsWith("capacityProviderInfo")) shouldBe empty
  }

  test("the allocated resources are clamped up to the configured minimums") {
    val (_, node) = fixture(
      capacityProviders = Nil,
      info = name => IO.pure(anyShape(name)),
      remaining = noRoom,
      run = _ => IO.pure(placed("task/workers/external"))
    )

    val allocated = place(node, request(cpu = 1, memory = 128))
      .map(_._2)
      .getOrElse(fail("expected a Right"))
    allocated.cpu shouldBe 1
    allocated.memory shouldBe 512
  }

  test("an image without a task definition fails before any placement") {
    val (calls, node) = fixture(
      capacityProviders = List("spot"),
      info = name => IO.pure(anyShape(name)),
      remaining = noRoom,
      run = _ => IO.pure(placed("task/workers/never"))
    )

    val withImage =
      request(cpu = 4, memory = 8000).copy(image = Some("unmapped:tag"))
    val result = node
      .requestOneNewJobFromJobScheduler(withImage)
      .unsafeRunSync()
    result.isLeft shouldBe true
    result.left
      .getOrElse(fail("expected a Left")) should include("unmapped:tag")
    calls.get.unsafeRunSync() shouldBe empty
  }
}
