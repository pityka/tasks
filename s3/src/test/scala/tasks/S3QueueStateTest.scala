package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import tasks.queue.QueueImpl
import tasks.queue.S3QueueState
import tasks.queue.SerializableQueueState
import tasks.util.message.LauncherName

class S3QueueStateTest extends FunSuite with Matchers {

  private val bucket = "queue-bucket"
  private val stateKey = "tasks-queue-state"

  private def transactionFor(client: FakeS3AsyncClient) =
    S3QueueState.makeTransaction(
      client = client,
      bucket = bucket,
      key = stateKey,
      retryBaseDelay = 1.millisecond
    )

  private def storedState(client: FakeS3AsyncClient): QueueImpl.State =
    SerializableQueueState.decode(
      S3QueueState.decompress(client.storedFor(stateKey).get._2)
    )

  test("an absent object reads as the empty state") {
    val client = new FakeS3AsyncClient()
    transactionFor(client)
      .use(_.get)
      .unsafeRunSync() shouldBe QueueImpl.State.empty
  }

  test("flatModify commits the new state and creates the object") {
    val client = new FakeS3AsyncClient()

    val result = transactionFor(client)
      .use(tx =>
        tx.flatModify(state =>
          (
            state.update(
              QueueImpl.LauncherJoined(LauncherName("launcher-1"), None)
            ),
            IO.pure(7)
          )
        )
      )
      .unsafeRunSync()

    result shouldBe 7
    client.storedFor(stateKey) shouldBe defined
    storedState(client).knownLaunchers.keySet shouldBe Set(
      LauncherName("launcher-1")
    )
  }

  test("the side effect runs only after the state object is committed") {
    val client = new FakeS3AsyncClient()
    val sideEffectRan = new AtomicInteger(0)
    val stateKeyPresentAtSideEffect = new AtomicInteger(-1)

    transactionFor(client)
      .use(tx =>
        tx.flatModify(state =>
          (
            state.update(
              QueueImpl.LauncherJoined(LauncherName("launcher-1"), None)
            ),
            IO {
              stateKeyPresentAtSideEffect.set(
                if (client.storedFor(stateKey).isDefined) 1 else 0
              )
              sideEffectRan.incrementAndGet()
            }
          )
        )
      )
      .unsafeRunSync()

    sideEffectRan.get shouldBe 1
    stateKeyPresentAtSideEffect.get shouldBe 1
  }

  test(
    "an update that leaves the state unchanged does not write the state object"
  ) {
    val client = new FakeS3AsyncClient()
    val sideEffectRan = new AtomicInteger(0)

    val result = transactionFor(client)
      .use(tx =>
        tx.flatModify(state =>
          (
            state,
            IO {
              sideEffectRan.incrementAndGet()
              3
            }
          )
        )
      )
      .unsafeRunSync()

    result shouldBe 3
    sideEffectRan.get shouldBe 1
    client.storedFor(stateKey) shouldBe None
  }

  test("a competing commit forces a retry and the update is reapplied") {
    val client = new FakeS3AsyncClient()
    val updateInvocations = new AtomicInteger(0)

    client.beforePut.set { () =>
      if (updateInvocations.get == 1)
        client.setStored(
          stateKey,
          (
            "\"competitor-etag\"",
            S3QueueState.compress(
              SerializableQueueState.encode(
                QueueImpl.State.empty.update(
                  QueueImpl.LauncherJoined(LauncherName("competitor"), None)
                )
              )
            )
          )
        )
    }

    transactionFor(client)
      .use(tx =>
        tx.flatModify { state =>
          updateInvocations.incrementAndGet()
          (
            state.update(
              QueueImpl.LauncherJoined(LauncherName("mine"), None)
            ),
            IO.unit
          )
        }
      )
      .unsafeRunSync()

    updateInvocations.get should be >= 2
    storedState(client).knownLaunchers.keySet shouldBe Set(
      LauncherName("competitor"),
      LauncherName("mine")
    )
  }

  test("compress and decompress round trip an encoded state") {
    val encoded = SerializableQueueState.encode(
      QueueImpl.State.empty.update(
        QueueImpl.Incremented(LauncherName("launcher-1"))
      )
    )
    S3QueueState.decompress(S3QueueState.compress(encoded)).toList shouldBe
      encoded.toList
  }

}
