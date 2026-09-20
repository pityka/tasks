package tasks

import cats.effect.unsafe.implicits.global

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import tasks.jsonitersupport._

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

object Utf8SharedFileTest extends TestHelpers {

  val payload = (0 until 100000).map(i => f"line-$i%09d").mkString("\n")

  case class Out(length: Int, matches: Boolean)
  object Out {
    implicit val codec: JsonValueCodec[Out] = JsonCodecMaker.make
  }

  val task = Task[Input, Out]("utf8sharedfile", 1) { _ => implicit env =>
    for {
      sf <- SharedFile.scoped(
        fs2.Stream.chunk(fs2.Chunk.array(payload.getBytes("UTF-8"))),
        "big.txt"
      )
      contents <- sf.utf8
    } yield Out(contents.length, contents == payload)
  }

  def run =
    withTaskSystem(testConfig) { implicit ts =>
      task(Input(1))(ResourceRequest(1, 500))
    }

}

class Utf8SharedFileTestSuite extends FunSuite with Matchers {

  test("SharedFile.utf8 returns the entire content of a multi-chunk file") {
    val out = Utf8SharedFileTest.run.unsafeRunSync().toOption.get
    out.length shouldBe Utf8SharedFileTest.payload.length
    out.matches shouldBe true
  }

}
