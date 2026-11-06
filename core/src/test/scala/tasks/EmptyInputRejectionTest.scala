package tasks

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class EmptyInputRejectionTest extends AnyFunSuite with Matchers {

  test("a task with a non-empty input compiles") {
    assertCompiles(
      """
      import tasks._
      import tasks.jsonitersupport._
      import com.github.plokhotnyuk.jsoniter_scala.macros._
      import com.github.plokhotnyuk.jsoniter_scala.core._
      import cats.effect.IO
      final case class NonEmpty(n: Int)
      object NonEmpty {
        implicit val c: JsonValueCodec[NonEmpty] = JsonCodecMaker.make
      }
      Task[NonEmpty, Int]("rejection-nonempty", 1) { in => implicit ce =>
        IO.pure(in.n)
      }
      """
    )
  }

  test("a task with an empty case class input does not compile") {
    assertDoesNotCompile(
      """
      import tasks._
      import tasks.jsonitersupport._
      import com.github.plokhotnyuk.jsoniter_scala.macros._
      import com.github.plokhotnyuk.jsoniter_scala.core._
      import cats.effect.IO
      final case class Empty()
      object Empty {
        implicit val c: JsonValueCodec[Empty] = JsonCodecMaker.make
      }
      Task[Empty, Int]("rejection-empty", 1) { in => implicit ce =>
        IO.pure(0)
      }
      """
    )
  }

  test("a task with a case object input does not compile") {
    assertDoesNotCompile(
      """
      import tasks._
      import tasks.jsonitersupport._
      import com.github.plokhotnyuk.jsoniter_scala.macros._
      import com.github.plokhotnyuk.jsoniter_scala.core._
      import cats.effect.IO
      case object Singleton
      object SingletonCodec {
        implicit val c: JsonValueCodec[Singleton.type] = JsonCodecMaker.make
      }
      import SingletonCodec._
      Task[Singleton.type, Int]("rejection-singleton", 1) { in => implicit ce =>
        IO.pure(0)
      }
      """
    )
  }

  test("a jsoniter-derived input feeds a task") {
    assertCompiles(
      """
      import tasks._
      import tasks.jsonitersupport._
      import com.github.plokhotnyuk.jsoniter_scala.macros._
      import cats.effect.IO
      final case class Thing(n: Int) derives ConfiguredJsonValueCodec
      Task[Thing, Int]("rejection-derives", 1) { in => implicit ce =>
        IO.pure(in.n)
      }
      """
    )
  }

}
