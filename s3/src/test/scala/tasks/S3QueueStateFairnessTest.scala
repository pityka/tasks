package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration._

import tasks.queue.QueueImpl
import tasks.queue.S3QueueState
import tasks.util.message.LauncherName

class S3QueueStateFairnessTest extends FunSuite with Matchers {

  private val bucket = "fair-bucket"
  private val stateKey = "fair-state"

  private def transactionFor(
      client: FakeS3AsyncClient,
      yieldDelay: FiniteDuration
  ) =
    S3QueueState.makeTransaction(
      client = client,
      bucket = bucket,
      key = stateKey,
      retryBaseDelay = 10.milliseconds,
      yieldDelay = yieldDelay
    )

  private def join(name: String): QueueImpl.Event =
    QueueImpl.LauncherJoined(LauncherName(name), None)

  test(
    "after a commit that had to retry past a 412, the next commit yields for yieldDelay"
  ) {
    val yieldDelay = 200.milliseconds
    val client = new FakeS3AsyncClient()

    val elapsed = transactionFor(client, yieldDelay)
      .use { tx =>
        tx.flatModify(state => (state.update(join("init")), IO.unit)) *> IO {
          val raceOnce = new AtomicBoolean(false)
          client.beforePut.set { () =>
            if (!raceOnce.getAndSet(true)) {
              val currentBytes = client.storedFor(stateKey).get._2
              client.setStored(stateKey, ("\"stranger\"", currentBytes))
            }
          }
        } *>
          tx.flatModify(state => (state.update(join("fight")), IO.unit)) *>
          IO.monotonic.flatMap { t0 =>
            tx.flatModify(state => (state.update(join("post")), IO.unit)) *>
              IO.monotonic.map(t1 => (t1 - t0).toMillis)
          }
      }
      .unsafeRunSync()

    elapsed should be >= (yieldDelay.toMillis - 20)
  }

  test(
    "a sequence of commits with no competitor finishes well under yieldDelay — no wasted sleeps"
  ) {
    val yieldDelay = 500.milliseconds
    val client = new FakeS3AsyncClient()

    val elapsed = transactionFor(client, yieldDelay)
      .use { tx =>
        tx.flatModify(state => (state.update(join("first")), IO.unit)) *>
          IO.monotonic.flatMap { t0 =>
            tx.flatModify(state => (state.update(join("second")), IO.unit)) *>
              tx.flatModify(state => (state.update(join("third")), IO.unit)) *>
              IO.monotonic.map(t1 => (t1 - t0).toMillis)
          }
      }
      .unsafeRunSync()

    elapsed should be < yieldDelay.toMillis
  }

  test(
    "if readLatest observes an unknown ETag committed by someone else, the next commit yields"
  ) {
    val yieldDelay = 150.milliseconds
    val client = new FakeS3AsyncClient()

    val elapsed = transactionFor(client, yieldDelay)
      .use { tx =>
        tx.flatModify(state => (state.update(join("seed")), IO.unit)) *> IO {
          val cur = client.storedFor(stateKey).get
          client.setStored(stateKey, ("\"other-main\"", cur._2))
        } *>
          IO.sleep(1500.milliseconds) *>
          IO.monotonic.flatMap { t0 =>
            tx.flatModify(state =>
              (state.update(join("after-read")), IO.unit)
            ) *>
              IO.monotonic.map(t1 => (t1 - t0).toMillis)
          }
      }
      .unsafeRunSync()

    elapsed should be >= (yieldDelay.toMillis - 20)
  }

}
