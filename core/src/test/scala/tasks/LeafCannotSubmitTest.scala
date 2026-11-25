package test

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

class LeafCannotSubmitTest extends FunSuite with Matchers {

  test("leaf body cannot summon ExtendedTaskSystemComponents") {
    """
    import tasks._
    import tasks.jsonitersupport._
    import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
    import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec

    object Fixture {
      case class In(i: Int)
      object In { implicit val c: JsonValueCodec[In] = JsonCodecMaker.make }
      val child = Task[In, Int]("child", 1) { in => implicit ce => cats.effect.IO.pure(in.i) }
      val parent = Task[In, Int]("parent", 1) { in => implicit ce =>
        child(In(0))(ResourceRequest(1, 100))
      }
    }
    """ shouldNot typeCheck
  }

  test(
    "leaf body cannot rebind ExtendedTaskSystemComponents from ce.components"
  ) {
    """
    import tasks._
    import tasks.jsonitersupport._
    import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
    import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec

    object Fixture {
      case class In(i: Int)
      object In { implicit val c: JsonValueCodec[In] = JsonCodecMaker.make }
      val parent = Task[In, Int]("parent", 1) { in => implicit ce =>
        implicit val smuggled: ExtendedTaskSystemComponents = ce.components
        cats.effect.IO.pure(in.i)
      }
    }
    """ shouldNot typeCheck
  }

  test(
    "leaf body cannot submit through a helper taking ExtendedTaskSystemComponents"
  ) {
    """
    import tasks._
    import tasks.jsonitersupport._
    import cats.effect.IO
    import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
    import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec

    object Fixture {
      case class In(i: Int)
      object In { implicit val c: JsonValueCodec[In] = JsonCodecMaker.make }
      val child = Task[In, Int]("child", 1) { in => implicit ce => IO.pure(in.i) }
      def runChild(a: In)(implicit tsc: ExtendedTaskSystemComponents): IO[Int] =
        child(a)(ResourceRequest(1, 100))
      val parent = Task[In, Int]("parent", 1) { in => implicit ce =>
        runChild(In(0))
      }
    }
    """ shouldNot typeCheck
  }

  test(
    "top-level with ExtendedTaskSystemComponents in scope can submit (positive control)"
  ) {
    """
    import tasks._
    import tasks.jsonitersupport._
    import cats.effect.IO
    import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec

    def go(child: TaskDefinition[Int, Int])(implicit tsc: ExtendedTaskSystemComponents, c: JsonValueCodec[Int]): IO[Int] =
      child(0)(ResourceRequest(1, 100))
    """ should compile
  }

  test(
    "leaf body can call plumbing helpers that take TaskSystemComponents (positive control)"
  ) {
    """
    import tasks._
    import tasks.queue.LeafComputationEnvironment
    import tasks.util.Uri
    import cats.effect.IO

    def readIt(uri: Uri)(implicit tsc: TaskSystemComponents): IO[String] =
      SharedFile(uri).flatMap(_.utf8)

    def body: LeafComputationEnvironment => IO[String] = { implicit ce =>
      readIt(Uri("file:///tmp/x"))
    }
    """ should compile
  }

}
