package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import tasks.util.LocalMessenger
import tasks.util.message._

class LocalMessengerTestSuite extends FunSuite with Matchers {

  test("concurrent subscribe on same address should not silently overwrite") {
    LocalMessenger.make
      .use { messenger =>
        val address = Address("test-address")
        val data: MessageData = MessageData.Ping

        for {
          // First subscribe succeeds
          stream1 <- messenger.subscribe(address)
          // Second subscribe on same address should fail
          result <- messenger.subscribe(address).attempt
        } yield {
          result.isLeft shouldBe true
          result.swap.getOrElse(fail()).getMessage should include(
            "already subscribed"
          )
        }
      }
      .unsafeRunSync()
  }

  test("concurrent subscribe race should not lose messages") {
    // Run many iterations to exercise the race window
    val iterations = 100
    val results = (1 to iterations).map { i =>
      LocalMessenger.make
        .use { messenger =>
          val address = Address(s"race-test-$i")
          val msg =
            Message(MessageData.Ping, from = Address.unknown, to = address)

          for {
            // Subscribe and immediately send a message
            stream <- messenger.subscribe(address)
            _ <- messenger.submit(msg)
            // The subscriber should receive the message
            received <- stream.take(1).compile.lastOrError
          } yield received.data shouldBe MessageData.Ping
        }
        .unsafeRunSync()
    }
  }

  test(
    "two racing subscribes to same address should not both succeed"
  ) {
    // Attempt concurrent subscribes — exactly one should succeed
    LocalMessenger.make
      .use { messenger =>
        val address = Address("race-addr")

        IO.both(
          messenger.subscribe(address).attempt,
          messenger.subscribe(address).attempt
        ).map { case (r1, r2) =>
          // Exactly one should succeed, one should fail
          val successes = List(r1, r2).count(_.isRight)
          val failures = List(r1, r2).count(_.isLeft)
          successes should equal(1)
          failures should equal(1)
        }
      }
      .unsafeRunSync()
  }

}
