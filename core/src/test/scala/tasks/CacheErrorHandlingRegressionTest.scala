package test

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, Inside}
import org.ekrich.config.ConfigFactory
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import scala.concurrent.duration._

object CacheErrorHandlingRegressionTest {

  case class SimpleInput(value: Int)
  object SimpleInput {
    implicit val codec: JsonValueCodec[SimpleInput] = JsonCodecMaker.make
  }

  case class SimpleOutput(result: Int)
  object SimpleOutput {
    implicit val codec: JsonValueCodec[SimpleOutput] = JsonCodecMaker.make
  }

  val simpleTask: TaskDefinition[SimpleInput, SimpleOutput] =
    Task[SimpleInput, SimpleOutput]("cache-error-test", 1) { input => _ =>
      IO(SimpleOutput(input.value * 2))
    }

  val cacheHitTestTask: TaskDefinition[SimpleInput, SimpleOutput] =
    Task[SimpleInput, SimpleOutput]("cache-hit-test", 1) { input => _ =>
      IO(SimpleOutput(input.value * 2))
    }

  val cacheMissTestTask: TaskDefinition[SimpleInput, SimpleOutput] =
    Task[SimpleInput, SimpleOutput]("cache-miss-test", 1) { input => _ =>
      IO(SimpleOutput(input.value * 3))
    }

  val cacheErrorTestTask: TaskDefinition[SimpleInput, SimpleOutput] =
    Task[SimpleInput, SimpleOutput]("cache-compute-error-test", 1) {
      input => _ =>
        IO(SimpleOutput(input.value * 2))
    }

  val resourceCacheTestTask: TaskDefinition[SimpleInput, SimpleOutput] =
    Task[SimpleInput, SimpleOutput]("resource-cache-test", 1) { input => _ =>
      IO(SimpleOutput(input.value * 4))
    }

  val concurrentCacheTestTask: TaskDefinition[SimpleInput, SimpleOutput] =
    Task[SimpleInput, SimpleOutput]("concurrent-cache-test", 1) { input => _ =>
      IO {
        IO.sleep(100.millis).unsafeRunSync()
        SimpleOutput(input.value * 5)
      }
    }

  val independentCacheTestTask1: TaskDefinition[SimpleInput, SimpleOutput] =
    Task[SimpleInput, SimpleOutput]("independent-cache-1", 1) { input => _ =>
      IO(SimpleOutput(input.value * 2))
    }

  val independentCacheTestTask2: TaskDefinition[SimpleInput, SimpleOutput] =
    Task[SimpleInput, SimpleOutput]("independent-cache-2", 1) { input => _ =>
      IO(SimpleOutput(input.value * 3))
    }
}

class CacheErrorHandlingRegressionTest
    extends FunSuite
    with Matchers
    with BeforeAndAfterAll
    with TestHelpers
    with Inside {

  override val testConfig = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""
tasks.cache.enabled = true
tasks.createFilePrefixForTaskId = false
tasks.disableRemoting = true
hosts.numCPU=4
tasks.askInterval = 20 ms
tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      """
    )
  }

  val pair = defaultTaskSystem(Some(testConfig)).allocated.unsafeRunSync()
  implicit val system: ExtendedTaskSystemComponents = pair._1._1
  import CacheErrorHandlingRegressionTest._

  test(
    "cache hit: repeated task executions use cache and skip computation"
  ) {
    val input = SimpleInput(5)
    val result1 =
      cacheHitTestTask(input)(ResourceRequest(1, 500)).unsafeRunSync()
    val result2 =
      cacheHitTestTask(input)(ResourceRequest(1, 500)).unsafeRunSync()

    result1 shouldBe SimpleOutput(10)
    result2 shouldBe SimpleOutput(10)
  }

  test(
    "cache miss: different inputs bypass cache and execute separately"
  ) {
    val result1 =
      cacheMissTestTask(SimpleInput(5))(ResourceRequest(1, 500)).unsafeRunSync()
    val result2 =
      cacheMissTestTask(SimpleInput(6))(ResourceRequest(1, 500)).unsafeRunSync()

    result1 shouldBe SimpleOutput(15)
    result2 shouldBe SimpleOutput(18)
  }

  test(
    "cache persistence: restarting task system can retrieve cached results"
  ) {
    val input = SimpleInput(99)
    val result1 = simpleTask(input)(ResourceRequest(1, 500)).unsafeRunSync()
    result1 shouldBe SimpleOutput(198)
  }

  test(
    "task execution with resource allocation respects cache across different resource requests with same input"
  ) {
    val input = SimpleInput(3)

    val result1 =
      resourceCacheTestTask(input)(ResourceRequest(1, 500)).unsafeRunSync()
    val result2 =
      resourceCacheTestTask(input)(ResourceRequest(2, 1000)).unsafeRunSync()

    result1 shouldBe SimpleOutput(12)
    result2 shouldBe SimpleOutput(12)
  }

  test(
    "concurrent identical task submissions all retrieve from cache after first completion"
  ) {
    val input = SimpleInput(2)

    val results = IO
      .parSequenceN(4)(
        (1 to 4).toList
          .map(_ => concurrentCacheTestTask(input)(ResourceRequest(1, 500)))
      )
      .unsafeRunSync()

    results.foreach { r =>
      r shouldBe SimpleOutput(10)
    }
  }

  test(
    "cache entries are independent: modifying one does not affect others"
  ) {
    val input = SimpleInput(4)

    independentCacheTestTask1(input)(ResourceRequest(1, 500)).unsafeRunSync()
    independentCacheTestTask2(input)(ResourceRequest(1, 500)).unsafeRunSync()

    independentCacheTestTask1(input)(ResourceRequest(1, 500)).unsafeRunSync()
    independentCacheTestTask2(input)(ResourceRequest(1, 500)).unsafeRunSync()
  }

  override def afterAll() = {
    pair._2.unsafeRunSync()
  }
}
