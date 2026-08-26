package tasks.elastic.ecs

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global

import tasks.elastic._
import tasks.shared._
import tasks.util.SimpleSocketAddress

object EcsNodeSelectorPlacementTest {

  import EcsPlacementChainTest.tasksConfig

  final class RecordingEcsOperations(
      specs: Ref[IO, List[(PlacementTarget, WorkerTaskSpec)]],
      run: PlacementTarget => IO[Either[List[TaskPlacementFailure], String]]
  ) extends EcsOperations {

    def runTask(
        spec: WorkerTaskSpec,
        target: PlacementTarget,
        clientToken: String
    ): IO[Either[List[TaskPlacementFailure], String]] =
      specs.update(_ :+ ((target, spec))) *> run(target)

    def stopTask(taskArn: String, reason: String): IO[Unit] = IO.unit

    def placeableCapacity(
        target: PlacementTarget
    ): IO[List[ContainerInstanceCapacity]] = IO.pure(Nil)

    def capacityProviderInfo(
        capacityProvider: String
    ): IO[CapacityProviderInfo] =
      IO.pure(EcsPlacementChainTest.anyShape(capacityProvider))
  }

  def fixture(
      capacityProviders: List[String],
      run: PlacementTarget => IO[Either[List[TaskPlacementFailure], String]]
  ): (Ref[IO, List[(PlacementTarget, WorkerTaskSpec)]], EcsCreateNode) = {
    val specs = Ref.unsafe[IO, List[(PlacementTarget, WorkerTaskSpec)]](Nil)
    val ops = new RecordingEcsOperations(specs, run)
    val node = new EcsCreateNode(
      masterAddress = SimpleSocketAddress("master", 1234),
      masterPrefix = "prefix",
      codeAddress =
        CodeAddress(SimpleSocketAddress("master", 1235), CodeVersion("1")),
      ops = ops,
      ecsConfig =
        EcsConfig("workers", capacityProviders, "worker", "worker-td"),
      resolvedRegion = "us-east-1"
    )
    (specs, node)
  }

  def request(selector: Option[NodeSelector]): ResourceRequest =
    ResourceRequest(cpu = 4, memory = 8000, scratch = 0, gpu = 0)
      .copy(nodeSelector = selector)

  def place(
      node: EcsCreateNode,
      requestSize: ResourceRequest
  ): Either[String, (PendingJobId, ResourceAvailable)] =
    node
      .requestOneNewJobFromJobScheduler(requestSize)(tasksConfig)
      .unsafeRunSync()
}

class EcsNodeSelectorPlacementTest extends AnyFunSuite with Matchers {

  import EcsNodeSelectorPlacementTest._
  import EcsPlacementChainTest.{placed, shortage}

  private val onExternal: PlacementTarget => IO[
    Either[List[TaskPlacementFailure], String]
  ] = {
    case PlacementTarget.External => IO.pure(placed("task/workers/external"))
    case _                        => IO.pure(shortage)
  }

  private val onProvider: PlacementTarget => IO[
    Either[List[TaskPlacementFailure], String]
  ] = {
    case PlacementTarget.CapacityProvider("gpu") =>
      IO.pure(placed("task/workers/gpu"))
    case _ => IO.pure(shortage)
  }

  private def specFor(
      recorded: List[(PlacementTarget, WorkerTaskSpec)],
      target: PlacementTarget
  ): WorkerTaskSpec =
    recorded
      .collectFirst { case (`target`, spec) => spec }
      .getOrElse(fail(s"RunTask was never attempted against $target"))

  test("a capacity provider placement carries the selector as a constraint") {
    val (specs, node) = fixture(List("gpu"), onProvider)

    place(node, request(Some(NodeSelector.Has("zone:a")))).map(_._1) shouldBe
      Right(PendingJobId("task/workers/gpu"))

    specFor(
      specs.get.unsafeRunSync(),
      PlacementTarget.CapacityProvider("gpu")
    ).placementExpression shouldBe Some("attribute:zone == a")
  }

  test("an external placement composes the selector with the external filter") {
    EcsOperations.constraintExpression(
      PlacementTarget.External,
      Some("attribute:zone == a")
    ) shouldBe Some(
      s"${EcsOperations.externalInstanceFilter} and (attribute:zone == a)"
    )
  }

  test("an external placement without a selector keeps the external filter") {
    EcsOperations.constraintExpression(
      PlacementTarget.External,
      None
    ) shouldBe Some(EcsOperations.externalInstanceFilter)
  }

  test("a capacity provider placement without a selector is unconstrained") {
    EcsOperations.constraintExpression(
      PlacementTarget.CapacityProvider("gpu"),
      None
    ) shouldBe None
  }

  test("every target in the chain gets the same constraint") {
    val (specs, node) = fixture(List("spot", "gpu"), onProvider)

    place(node, request(Some(NodeSelector.Has("gpu"))))

    val recorded = specs.get.unsafeRunSync()
    recorded.map(_._1) shouldBe List(
      PlacementTarget.External,
      PlacementTarget.CapacityProvider("spot"),
      PlacementTarget.CapacityProvider("gpu")
    )
    recorded.map(_._2.placementExpression).distinct shouldBe List(
      Some("attribute:gpu exists")
    )
  }

  test("a request without a selector carries no constraint") {
    val (specs, node) = fixture(Nil, onExternal)

    place(node, request(None))

    specFor(
      specs.get.unsafeRunSync(),
      PlacementTarget.External
    ).placementExpression shouldBe None
  }

  test("an unmappable selector fails before any placement is attempted") {
    val (specs, node) = fixture(List("gpu"), onProvider)

    val result = place(node, request(Some(NodeSelector.Has("zone a"))))

    result.isLeft shouldBe true
    result.left
      .getOrElse(fail("expected a Left")) should include(
      "not a valid attribute name"
    )
    specs.get.unsafeRunSync() shouldBe empty
  }

  test("an unsatisfiable selector fails before any placement is attempted") {
    val (specs, node) = fixture(List("gpu"), onProvider)

    val result = place(node, request(Some(NodeSelector.Or(Nil))))

    result.isLeft shouldBe true
    result.left
      .getOrElse(fail("expected a Left")) should include(
      "can never be satisfied"
    )
    specs.get.unsafeRunSync() shouldBe empty
  }

  test(
    "an ATTRIBUTE rejection is reported as unretryable and names the constraint"
  ) {
    val (_, node) = fixture(
      Nil,
      _ => IO.pure(Left(List(TaskPlacementFailure("ATTRIBUTE", None))))
    )

    val result = place(node, request(Some(NodeSelector.Has("zone:a"))))

    val message = result.left.getOrElse(fail("expected a Left"))
    message should include("attribute:zone == a")
    message should include("retrying this request cannot help")
    message should include("ECS_INSTANCE_ATTRIBUTES")
  }
}
