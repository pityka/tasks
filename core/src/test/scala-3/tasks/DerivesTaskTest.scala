package tasks

import org.scalatest.funsuite.AnyFunSuite

import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import cats.effect.IO

object DerivesTaskFixture {
  final case class Thing(n: Int) derives ConfiguredJsonValueCodec
  val t =
    Task[Thing, Int]("rejection-derives", 1) { in => implicit ce =>
      IO.pure(in.n)
    }
}

class DerivesTaskTest extends AnyFunSuite {
  test("a jsoniter-derived input feeds a task") {
    assert(DerivesTaskFixture.t ne null)
  }
}
