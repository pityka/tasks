package tasks.elastic.ecs

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import cats.effect.unsafe.implicits.global

import tasks.shared.NodeSelector

class EcsInstanceLabelsTest extends AnyFunSuite with Matchers {

  private def valued(name: String, value: String) = (name, Some(value))

  private def bare(name: String) = (name, Option.empty[String])

  private def advertised(
      attributes: List[(String, Option[String])]
  ): Set[String] =
    EcsInstanceLabels.advertised(attributes, Set.empty)

  test("a custom valued attribute becomes a name:value label") {
    advertised(List(valued("zone", "a"))) shouldBe Set("zone:a")
  }

  test("a custom valueless attribute becomes a bare label") {
    advertised(List(bare("gpu"))) shouldBe Set("gpu")
  }

  test("an attribute with an empty value becomes a bare label") {
    advertised(List(valued("gpu", ""))) shouldBe Set("gpu")
  }

  test("built in ecs attributes are not advertised by default") {
    advertised(
      List(
        valued("ecs.instance-type", "g5.xlarge"),
        valued("ecs.capability.docker-plugin.local", ""),
        valued("zone", "a")
      )
    ) shouldBe Set("zone:a")
  }

  test("a built in attribute is advertised once it is opted in by name") {
    EcsInstanceLabels.advertised(
      List(
        valued("ecs.instance-type", "g5.xlarge"),
        valued("ecs.availability-zone", "us-east-1a")
      ),
      Set("ecs.instance-type")
    ) shouldBe Set("ecs.instance-type:g5.xlarge")
  }

  test("a value containing a colon survives, since only the first splits") {
    advertised(List(valued("build", "2026:08"))) shouldBe Set("build:2026:08")
  }

  test("an attribute whose value cannot round trip is dropped") {
    advertised(List(valued("zone", "us east 1a"))) shouldBe empty
  }

  test("an attribute whose name cannot round trip is dropped") {
    advertised(List(valued("zone one", "a"))) shouldBe empty
  }

  test("a droppable attribute does not take the others with it") {
    advertised(
      List(valued("zone", "us east 1a"), valued("rack", "r1"))
    ) shouldBe Set("rack:r1")
  }

  test("every advertised label maps back to a placement expression") {
    val labels = EcsInstanceLabels.advertised(
      List(
        valued("zone", "a"),
        bare("gpu"),
        valued("build", "2026:08"),
        valued("ecs.instance-type", "g5.xlarge")
      ),
      Set("ecs.instance-type")
    )

    labels should have size 4
    labels.foreach { label =>
      EcsAttributes.placementExpression(
        Some(NodeSelector.Has(label))
      ) shouldBe a[Right[_, _]]
    }
  }

  test("no metadata uri means no attributes are advertised") {
    EcsInstanceLabels
      .discover(
        ecs = null,
        ecsConfig = EcsConfig("workers", "worker", "worker-td"),
        metadataUri = None
      )
      .unsafeRunSync() shouldBe empty
  }
}
