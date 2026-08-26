package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import scala.concurrent.duration._

import tasks.util.Actor
import tasks.util.LocalMessenger
import tasks.util.message._

class StreamFailureIsLoggedTest extends FunSuite with Matchers {

  private def capturingErrors[A](body: => A): (A, List[String]) = {
    val captured = scala.collection.mutable.ArrayBuffer.empty[String]
    val handler = scribe.handler.LogHandler(scribe.Level.Error) { record =>
      captured.synchronized {
        val _ = captured += record.logOutput.plainText
      }
    }
    scribe.Logger.root
      .clearHandlers()
      .clearModifiers()
      .withHandler(handler)
      .replace()
    try {
      val a = body
      (a, captured.synchronized(captured.toList))
    } finally {
      scribe.Logger.root
        .clearHandlers()
        .clearModifiers()
        .withHandler(minimumLevel = Some(scribe.Level.Warn))
        .replace()
    }
  }

  test(
    "an actor whose scheduler stream fails logs the failure instead of dying silently"
  ) {
    val failingScheduler: Actor.StopQueue => Option[IO[fs2.Stream[IO, Unit]]] =
      _ =>
        Some(
          IO(
            fs2.Stream.eval(
              IO.raiseError[Unit](new RuntimeException("scheduler boom"))
            )
          )
        )

    val (_, errors) = capturingErrors {
      LocalMessenger.make
        .flatMap { messenger =>
          Actor.make(
            schedulers = failingScheduler,
            receive = _ => PartialFunction.empty,
            address = Address("stream-failure-test-actor", None),
            messenger = messenger
          )
        }
        .use(_ => IO.sleep(300.millis))
        .unsafeRunSync()
    }

    errors.exists(_.contains("Actor stream failed")) shouldBe true
    errors.exists(_.contains("scheduler boom")) shouldBe true
  }

}
