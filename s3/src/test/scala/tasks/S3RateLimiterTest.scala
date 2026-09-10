package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global

import scala.concurrent.duration._

import scribe.Level
import scribe.handler.LogHandler

import tasks.queue.S3QueueState

class S3RateLimiterTest extends FunSuite with Matchers {

  test("the token bucket admits a burst then throttles to the refill rate") {
    val program =
      S3QueueState.rateLimiter(maxRequestsPerSecond = 10, maxBurst = 3).use {
        limiter =>
          for {
            start <- IO.monotonic
            _ <- limiter(IO.unit)
            _ <- limiter(IO.unit)
            _ <- limiter(IO.unit)
            afterBurst <- IO.monotonic
            _ <- limiter(IO.unit)
            afterFourth <- IO.monotonic
          } yield (afterBurst - start, afterFourth - afterBurst)
      }

    val (burstElapsed, fourthWait) =
      TestControl.executeEmbed(program).unsafeRunSync()

    burstElapsed shouldBe 0.seconds
    fourthWait shouldBe 100.milliseconds
  }

  test("the bucket does not accumulate tokens beyond the burst while idle") {
    val program =
      S3QueueState.rateLimiter(maxRequestsPerSecond = 10, maxBurst = 2).use {
        limiter =>
          for {
            _ <- IO.sleep(950.milliseconds)
            _ <- limiter(IO.unit)
            _ <- limiter(IO.unit)
            start <- IO.monotonic
            _ <- limiter(IO.unit)
            end <- IO.monotonic
          } yield end - start
      }

    TestControl.executeEmbed(program).unsafeRunSync() shouldBe 100.milliseconds
  }

  test("warns once (throttled) when backpressure kicks in") {
    val captured = scala.collection.mutable.ListBuffer.empty[scribe.LogRecord]
    val handler =
      LogHandler(Level.Warn)(record => captured.synchronized(captured += record))
    val previousRoot = scribe.Logger.root
    scribe.Logger.root.withHandler(handler).replace()

    try {
      val program =
        S3QueueState.rateLimiter(maxRequestsPerSecond = 10, maxBurst = 1).use {
          limiter =>
            limiter(IO.unit) *> limiter(IO.unit) *> limiter(IO.unit)
        }
      TestControl.executeEmbed(program).unsafeRunSync()
    } finally previousRoot.replace()

    val warnings =
      captured.synchronized(captured.toList).filter(_.level == Level.Warn)
    warnings.size shouldBe 1
    warnings.head.logOutput.plainText should include("rate-limited")
  }

  test("rejects a non-positive configuration") {
    intercept[IllegalArgumentException] {
      TestControl
        .executeEmbed(
          S3QueueState.rateLimiter(maxRequestsPerSecond = 0, maxBurst = 1).use_
        )
        .unsafeRunSync()
    }
  }

}
