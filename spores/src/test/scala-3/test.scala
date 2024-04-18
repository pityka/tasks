package tasks.queue

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
object Macro {
  val spore2 = SporeMacros.spore[Option[Int], Int] { case Some(a) => (a + 1) }
  val spore = SporeMacros.spore { (a: Int) =>
    val c = spore2(Option(a))
    spore2(Option(c)).toString
  }
  val lambda = SporeMacros.spore((a: Int) => (b: Int) => a + b)
}

class MacrosSpec extends FunSuite {
  test("spore within spore") {
    // assertEquals(Macros.location.line, 3)
    assert(Macro.spore(5) == "7")
    assert(Macro.lambda(5)(3) == 8)
  }
}
