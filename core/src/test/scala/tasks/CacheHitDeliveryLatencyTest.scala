package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should._
import org.ekrich.config.ConfigFactory

import tasks.jsonitersupport._

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import cats.effect.unsafe.implicits.global

import scala.concurrent.duration._

object CacheHitDeliveryLatencyTest {
  case class In(i: Int)
  object In {
    implicit val codec: JsonValueCodec[In] = JsonCodecMaker.make
  }
  case class Out(i: Int)
  object Out {
    implicit val codec: JsonValueCodec[Out] = JsonCodecMaker.make
  }

  val increment: TaskDefinition[In, Out] =
    Task[In, Out]("cache-hit-delivery-latency", 1) { in => _ =>
      IO.pure(Out(in.i + 1))
    }
}

class CacheHitDeliveryLatencyTestSuite
    extends FunSuite
    with Matchers
    with TestHelpers {

  import CacheHitDeliveryLatencyTest._

  def config(
      askInterval: String,
      resultPollInterval: String,
      storageURI: String
  ) =
    ConfigFactory.parseString(
      s"""
tasks.cache.enabled = true
tasks.disableRemoting = true
hosts.numCPU = 4
tasks.askInterval = $askInterval
tasks.resultPollInterval = $resultPollInterval
tasks.failuredetector.heartbeat-interval = 200 ms
tasks.fileservice.storageURI=$storageURI
"""
    )

  test(
    "a cache hit is delivered on the result-poll interval, not the ask interval"
  ) {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    val storage = tmp.getAbsolutePath

    val populated = withTaskSystem(Some(config("20 ms", "20 ms", storage))) {
      implicit ts =>
        increment(In(1))(ResourceRequest(1, 500)).void
    }.unsafeRunSync()
    populated shouldBe Right(())

    val measured =
      withTaskSystem(Some(config("30 seconds", "200 ms", storage))) {
        implicit ts =>
          for {
            start <- IO.monotonic
            out <- increment(In(1))(ResourceRequest(1, 500))
            end <- IO.monotonic
          } yield (out, end - start)
      }.unsafeRunSync()

    measured match {
      case Right((out, elapsed)) =>
        out shouldBe Out(2)
        withClue(
          s"cache hit took $elapsed, expected well under the 30s ask interval: "
        ) {
          assert(elapsed < 10.seconds)
        }
      case Left(exitCode) =>
        fail(s"task system exited before returning the result: $exitCode")
    }
  }
}
