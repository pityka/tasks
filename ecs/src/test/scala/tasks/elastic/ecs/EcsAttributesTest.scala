package tasks.elastic.ecs

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import tasks.shared.NodeSelector

class EcsAttributesTest extends AnyFunSuite with Matchers {

  import NodeSelector._

  private def expression(selector: NodeSelector): Option[String] =
    EcsAttributes
      .placementExpression(Some(selector))
      .getOrElse(fail(s"expected a mappable selector, got a rejection"))

  private def rejection(selector: NodeSelector): String =
    EcsAttributes
      .placementExpression(Some(selector))
      .swap
      .getOrElse(fail("expected the selector to be rejected"))

  test("a valued label becomes an equality test on the attribute") {
    expression(Has("zone:a")) shouldBe Some("attribute:zone == a")
  }

  test("a bare label becomes a presence test on the attribute") {
    expression(Has("gpu")) shouldBe Some("attribute:gpu exists")
  }

  test("only the first colon separates the name from the value") {
    expression(Has("build:2026:08")) shouldBe Some("attribute:build == 2026:08")
  }

  test("no selector at all constrains nothing") {
    EcsAttributes.placementExpression(None) shouldBe Right(None)
  }

  test("Always constrains nothing") {
    expression(Always) shouldBe None
  }

  test("Not negates the inner expression") {
    expression(Not(Has("zone:a"))) shouldBe Some("not(attribute:zone == a)")
  }

  test("And joins its clauses") {
    expression(And(List(Has("zone:a"), Has("gpu")))) shouldBe
      Some("(attribute:zone == a) and (attribute:gpu exists)")
  }

  test("Or joins its clauses") {
    expression(Or(List(Has("zone:a"), Has("zone:b")))) shouldBe
      Some("(attribute:zone == a) or (attribute:zone == b)")
  }

  test("an unconstrained clause drops out of an And") {
    expression(And(List(Always, Has("zone:a")))) shouldBe
      Some("attribute:zone == a")
  }

  test("an unconstrained clause makes the whole Or unconstrained") {
    expression(Or(List(Always, Has("zone:a")))) shouldBe None
  }

  test("an empty And constrains nothing") {
    expression(And(Nil)) shouldBe None
  }

  test("nesting composes without losing the grouping") {
    expression(
      And(List(Has("zone:a"), Not(Or(List(Has("spot"), Has("drain"))))))
    ) shouldBe Some(
      "(attribute:zone == a) and " +
        "(not((attribute:spot exists) or (attribute:drain exists)))"
    )
  }

  test("an empty Or is rejected because no instance could ever match") {
    rejection(Or(Nil)) should include("can never be satisfied")
  }

  test("negating a selector which matches everything is rejected") {
    rejection(Not(Always)) should include("can never be satisfied")
  }

  test("a label whose name is not a legal attribute name is rejected") {
    rejection(Has("zone a:x")) should include("not a valid attribute name")
  }

  test("a label whose value is not a legal attribute value is rejected") {
    rejection(Has("zone:a b")) should include("not a valid attribute value")
  }

  test("an over long attribute name is rejected") {
    val name = "a" * (EcsAttributes.maximumAttributeLength + 1)
    rejection(Has(s"$name:x")) should include("too long")
  }

  test("an over long attribute value is rejected") {
    val value = "a" * (EcsAttributes.maximumAttributeLength + 1)
    rejection(Has(s"zone:$value")) should include("too long")
  }

  test("a rejection anywhere in a composite fails the whole selector") {
    rejection(And(List(Has("zone:a"), Has("bad name")))) should include(
      "not a valid attribute name"
    )
  }

  test("labelOf and attributeOf are inverses for a valued attribute") {
    val label = EcsAttributes.labelOf("zone", "a")
    label shouldBe "zone:a"
    EcsAttributes.attributeOf(label) shouldBe Right(("zone", Some("a")))
  }

  test("labelOf and attributeOf are inverses for a valueless attribute") {
    val label = EcsAttributes.labelOf("gpu")
    label shouldBe "gpu"
    EcsAttributes.attributeOf(label) shouldBe Right(("gpu", None))
  }

  test("an attribute with an empty value renders as a bare label") {
    EcsAttributes.labelOf("gpu", "") shouldBe "gpu"
  }

  test(
    "a label built from an attribute produces the expression which selects it"
  ) {
    expression(Has(EcsAttributes.labelOf("ecs.instance-type", "g5.xlarge")))
      .shouldBe(Some("attribute:ecs.instance-type == g5.xlarge"))
  }
}
