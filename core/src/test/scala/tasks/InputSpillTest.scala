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

  case class BigOut(xs: List[Int])
  object BigOut {
    implicit val codec: JsonValueCodec[BigOut] = JsonCodecMaker.make
  }

  val echoTask: TaskDefinition[In, BigOut] =
    Task[In, BigOut]("output-spill-echo", 1) { in => _ =>
      IO.pure(BigOut(in.xs))
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

  test("a task output larger than maxInlinePayloadBytes spills and round-trips") {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    val storage = tmp.getAbsolutePath
    val input = In((1 to 1000).toList)

    val result = withTaskSystem(Some(config(storage, "1"))) { implicit ts =>
      echoTask(input)(ResourceRequest(1, 500))
    }.unsafeRunSync()

    result shouldBe Right(BigOut(input.xs))
  }

  test("a spilled output round-trips on a cache hit") {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    val storage = tmp.getAbsolutePath
    val input = In((1 to 1000).toList)

    val first = withTaskSystem(Some(config(storage, "1"))) { implicit ts =>
      echoTask(input)(ResourceRequest(1, 500))
    }.unsafeRunSync()
    first shouldBe Right(BigOut(input.xs))

    val second = withTaskSystem(Some(config(storage, "1"))) { implicit ts =>
      echoTask(input)(ResourceRequest(1, 500))
    }.unsafeRunSync()
    second shouldBe Right(BigOut(input.xs))
  }

  test("UntypedResult codec omits spilled=None and decodes an absent spilled as None") {
    val sf = tasks.fileservice.SharedFileHelper.createForTesting("x")
    val inline = tasks.queue.UntypedResult(
      Set(sf),
      tasks.queue.Base64Data("YWJj"),
      None,
      None
    )
    val bytes = writeToArray(inline)(tasks.queue.UntypedResult.codec)
    val json = new String(bytes, "UTF-8")
    assert(!json.contains("spilled"))
    val decoded =
      readFromArray[tasks.queue.UntypedResult](bytes)(
        tasks.queue.UntypedResult.codec
      )
    decoded shouldBe inline
    decoded.spilled shouldBe None
  }
}
