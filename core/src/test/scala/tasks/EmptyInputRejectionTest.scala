package tasks

import org.scalatest.funsuite.AnyFunSuite

import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

object EmptyInputRejectionFixtures {
  final case class NonEmpty(n: Int)
  implicit val nonEmptyCodec: JsonValueCodec[NonEmpty] =
    new JsonValueCodec[NonEmpty] {
      def nullValue: NonEmpty = null
      def decodeValue(in: JsonReader, default: NonEmpty): NonEmpty =
        NonEmpty(in.readInt())
      def encodeValue(x: NonEmpty, out: JsonWriter): Unit =
        out.writeVal(x.n)
    }

  val nonEmptyTask =
    Task[NonEmpty, Int]("rejection-nonempty", 1) { in => implicit ce =>
      IO.pure(in.n)
    }
}

object EmptyInputHatchFixtures {
  import tasks.unsafe.allowEmptyInput
  final case class EmptyOk()
  implicit val emptyOkCodec: JsonValueCodec[EmptyOk] =
    new JsonValueCodec[EmptyOk] {
      def nullValue: EmptyOk = null
      def decodeValue(in: JsonReader, default: EmptyOk): EmptyOk = EmptyOk()
      def encodeValue(x: EmptyOk, out: JsonWriter): Unit = out.writeVal(0)
    }

  val hatchTask =
    Task[EmptyOk, Int]("rejection-hatch", 1) { in => implicit ce =>
      IO.pure(0)
    }
}

class EmptyInputRejectionTest extends AnyFunSuite {

  test("a task with a non-empty input compiles") {
    assert(EmptyInputRejectionFixtures.nonEmptyTask ne null)
  }

  test("tasks.unsafe.allowEmptyInput permits an empty input") {
    assert(EmptyInputHatchFixtures.hatchTask ne null)
  }

  test("a task with an empty case class input does not compile") {
    assertDoesNotCompile(
      """
      import tasks._
      import tasks.jsonitersupport._
      import com.github.plokhotnyuk.jsoniter_scala.core._
      import cats.effect.IO
      final case class Empty()
      implicit val emptyCodec: JsonValueCodec[Empty] =
        new JsonValueCodec[Empty] {
          def nullValue: Empty = null
          def decodeValue(in: JsonReader, default: Empty): Empty = Empty()
          def encodeValue(x: Empty, out: JsonWriter): Unit = out.writeVal(0)
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
      import com.github.plokhotnyuk.jsoniter_scala.core._
      import cats.effect.IO
      case object Singleton
      implicit val singletonCodec: JsonValueCodec[Singleton.type] =
        new JsonValueCodec[Singleton.type] {
          def nullValue: Singleton.type = null
          def decodeValue(in: JsonReader, default: Singleton.type): Singleton.type =
            Singleton
          def encodeValue(x: Singleton.type, out: JsonWriter): Unit = out.writeVal(0)
        }
      Task[Singleton.type, Int]("rejection-singleton", 1) { in => implicit ce =>
        IO.pure(0)
      }
      """
    )
  }

}
