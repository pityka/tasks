package tasks.queue

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers
import tasks.queue.SporeMacros.spore

object SporeMacroPositiveFixtures {
  val inner: Spore[Int, Int] = spore((a: Int) => a + 1)
  val outer: Spore[Int, Int] = spore((a: Int) => inner(a) + 10)
  val outerManyRefs: Spore[Int, Int] =
    spore((a: Int) => inner(a) + inner(a) + inner(0))

  val nullary: Spore[Unit, Int] = spore((_: Unit) => 42)
  val outerUsingNullary: Spore[Int, Int] =
    spore((a: Int) => a + nullary(()))

  def mkSpore[T]: Spore[List[T], String] =
    spore((a: List[T]) => a.toString)
  val forString: Spore[List[String], String] = mkSpore[String]
  val forInt: Spore[List[Int], String] = mkSpore[Int]

  val topLevel: Spore[Int, Int] = spore((a: Int) => a + 1)
}

class SporeMacroNegativeTest extends FunSuite with Matchers {

  test("spore defined inside a class should not compile (failIfInner)") {
    """
    import tasks.queue.SporeMacros.spore

    class Outer {
      val sp = spore((a: String) => a.toUpperCase)
    }
    """ shouldNot typeCheck
  }

  test("spore defined inside an anonymous class should not compile") {
    """
    import tasks.queue.SporeMacros.spore

    val anon = new java.util.AbstractList[Int] {
      val sp = spore((a: Int) => a + 1)
      def get(i: Int) = sp(i)
      def size() = 0
    }
    """ shouldNot typeCheck
  }

  test(
    "spore closing over a local val from a method scope should not compile"
  ) {
    """
    import tasks.queue.SporeMacros.spore

    object Fixture {
      def foo: Int => Int = {
        val x = 7
        spore((a: Int) => a + x)
      }
    }
    """ shouldNot typeCheck
  }

  test("spore closing over a method parameter should not compile") {
    """
    import tasks.queue.SporeMacros.spore

    object Fixture {
      def foo(x: Int): Int => Int = spore((a: Int) => a + x)
    }
    """ shouldNot typeCheck
  }

  test(
    "spore closing over a non-Spore outer value from an object should not compile"
  ) {
    """
    import tasks.queue.SporeMacros.spore

    object Fixture {
      val x: Int = 7
      val sp = spore((a: Int) => a + x)
    }
    """ shouldNot typeCheck
  }

  test("spore passed something that is not a lambda should not compile") {
    """
    import tasks.queue.SporeMacros.spore

    object Fixture {
      def mult(a: Int): Int = a * 2
      val sp = spore(mult)
    }
    """ shouldNot typeCheck
  }

  test("spore with a two-parameter lambda should not compile") {
    """
    import tasks.queue.SporeMacros.spore

    object Fixture {
      val sp = spore((a: Int, b: Int) => a + b)
    }
    """ shouldNot typeCheck
  }

  test("spore directly referencing another spore evaluates (positive)") {
    SporeMacroPositiveFixtures.outer(5) shouldBe 16
  }

  test("spore referencing another spore multiple times evaluates (positive)") {
    SporeMacroPositiveFixtures.outerManyRefs(3) shouldBe 9
  }

  test("spore referencing a nullary spore (Unit-input) evaluates (positive)") {
    SporeMacroPositiveFixtures.outerUsingNullary(8) shouldBe 50
  }

  test("generic method producing a spore evaluates (positive)") {
    SporeMacroPositiveFixtures.forString(List("a", "b")) shouldBe "List(a, b)"
    SporeMacroPositiveFixtures.forInt(List(1, 2)) shouldBe "List(1, 2)"
  }

  test("spore at object top level with no captures evaluates (positive)") {
    SporeMacroPositiveFixtures.topLevel(41) shouldBe 42
  }

}
