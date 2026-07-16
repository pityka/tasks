package smithy.test

import smithy4s.{Document, Hints, Schema, ShapeId, ShapeTag}

// Stubs for smithy smoke-test traits referenced by generated AWS code.
// smithy4s 0.19.2 does not ship Scala types for @smokeTests but the AWS models use them.
// Dead code at runtime — only kept to satisfy the compiler.

final case class SmokeTests(value: List[SmokeTestCase])
object SmokeTests extends ShapeTag.Companion[SmokeTests] {
  val id: ShapeId = ShapeId("smithy.test", "smokeTests")
  implicit val schema: Schema[SmokeTests] =
    Schema
      .bijection[Document, SmokeTests](
        Schema.document,
        (_: Document) => SmokeTests(Nil),
        (_: SmokeTests) => Document.DNull
      )
      .withId(id)
}

final case class SmokeTestCase(
    id: String,
    expect: Expectation,
    params: Option[Document] = None,
    vendorParams: Option[Document] = None,
    vendorParamsShape: Option[ShapeId] = None,
    tags: Option[List[String]] = None
)

sealed trait Expectation { def widen: Expectation = this }
object Expectation {
  case object SuccessCase extends Expectation
  final case class FailureCase(failure: FailureExpectation) extends Expectation
}

final case class FailureExpectation(errorId: Option[ShapeId] = None)
