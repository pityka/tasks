package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should._
import org.ekrich.config.ConfigFactory

import tasks._
import tasks.jsonitersupport._

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import cats.effect.unsafe.implicits.global

object InputSpillTest {
  case class In(xs: List[Int])
  object In {
    implicit val codec: JsonValueCodec[In] = JsonCodecMaker.make
  }
  case class Out(sum: Int)
  object Out {
    implicit val codec: JsonValueCodec[Out] = JsonCodecMaker.make
  }

  val sumTask: TaskDefinition[In, Out] =
    Task[In, Out]("input-spill-sum", 1) { in => _ =>
      IO.pure(Out(in.xs.sum))
    }
}

class InputSpillTestSuite extends FunSuite with Matchers with TestHelpers {
  import InputSpillTest._

  def config(storageURI: String, maxInline: String) =
    ConfigFactory.parseString(
      s"""
tasks.cache.enabled = true
tasks.disableRemoting = true
hosts.numCPU = 4
tasks.maxInlinePayloadBytes = $maxInline
tasks.fileservice.storageURI=$storageURI
"""
    )

  test("a task input larger than maxInlinePayloadBytes spills and round-trips") {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    val storage = tmp.getAbsolutePath
    val input = In((1 to 1000).toList)

    val result = withTaskSystem(Some(config(storage, "1"))) { implicit ts =>
      sumTask(input)(ResourceRequest(1, 500))
    }.unsafeRunSync()

    result shouldBe Right(Out(input.xs.sum))
  }

  test("the same input hits the cache whether first inlined or spilled") {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    val storage = tmp.getAbsolutePath
    val input = In((1 to 1000).toList)

    val inlined = withTaskSystem(Some(config(storage, "1 GiB"))) {
      implicit ts =>
        sumTask(input)(ResourceRequest(1, 500))
    }.unsafeRunSync()
    inlined shouldBe Right(Out(input.xs.sum))

    val spilledHit = withTaskSystem(Some(config(storage, "1"))) { implicit ts =>
      sumTask(input)(ResourceRequest(1, 500))
    }.unsafeRunSync()
    spilledHit shouldBe Right(Out(input.xs.sum))
  }
}
