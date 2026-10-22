package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import tasks.queue.QueueImpl
import tasks.queue.S3QueueState
import tasks.util.message.LauncherName

class S3QueueStateLeaseTest extends FunSuite with Matchers {

  private val bucket = "lease-bucket"
  private val stateKey = "lease-state"
  private val leaseKey = stateKey + ".lease"

  private def leaseConfigWith(
      ttl: FiniteDuration = 10.seconds,
      acquireDelay: FiniteDuration = 10.milliseconds
  ): S3QueueState.LeaseConfig =
    S3QueueState.LeaseConfig(
      key = leaseKey,
      ttl = ttl,
      acquireBaseDelay = acquireDelay
    )

  private def tx(
      client: FakeS3AsyncClient,
      lease: Option[S3QueueState.LeaseConfig] = Some(leaseConfigWith())
  ) =
    S3QueueState.makeTransaction(
      client = client,
      bucket = bucket,
      key = stateKey,
      retryBaseDelay = 10.milliseconds,
      leaseConfig = lease
    )

  test(
    "a leased flatModify acquires the lease, writes state, and deletes the lease"
  ) {
    val client = new FakeS3AsyncClient()
    val finalState = tx(client)
      .use { t =>
        t.flatModify(state =>
          (
            state.update(QueueImpl.LauncherJoined(LauncherName("a"), None)),
            IO.unit
          )
        ) *> t.get
      }
      .unsafeRunSync()
    finalState.knownLaunchers.keySet shouldBe Set(LauncherName("a"))
    client.storedFor(leaseKey) shouldBe None
    client.storedFor(stateKey) shouldBe defined
    client.deleteCount.get shouldBe 1
  }

  private def pair(client: FakeS3AsyncClient, cfg: S3QueueState.LeaseConfig) =
    tx(client, Some(cfg)).flatMap(a => tx(client, Some(cfg)).map(b => (a, b)))

  test(
    "two transactions sharing a bucket both commit under a lease without any 412 on the state key"
  ) {
    val client = new FakeS3AsyncClient()
    val cfg = leaseConfigWith()
    val stored = pair(client, cfg)
      .use { case (a, b) =>
        val joinA = a.flatModify(state =>
          (
            state.update(QueueImpl.LauncherJoined(LauncherName("A"), None)),
            IO.unit
          )
        )
        val joinB = b.flatModify(state =>
          (
            state.update(QueueImpl.LauncherJoined(LauncherName("B"), None)),
            IO.unit
          )
        )
        joinA.both(joinB)
      }
      .unsafeRunSync()
    val _ = stored
    val persisted = client.storedFor(stateKey).get
    val decoded = tasks.queue.SerializableQueueState.decode(
      S3QueueState.decompress(persisted._2)
    )
    decoded.knownLaunchers.keySet shouldBe Set(
      LauncherName("A"),
      LauncherName("B")
    )
  }

  test("an expired lease is stolen by a later acquirer") {
    val client = new FakeS3AsyncClient()
    val cfg =
      leaseConfigWith(ttl = 50.milliseconds, acquireDelay = 20.milliseconds)

    val finalState = pair(client, cfg)
      .use { case (a, b) =>
        for {
          _ <- a.flatModify(state =>
            (
              state.update(
                QueueImpl.LauncherJoined(LauncherName("first"), None)
              ),
              IO.unit
            )
          )
          _ <- IO {
            val payload = com.github.plokhotnyuk.jsoniter_scala.core
              .writeToArray(S3QueueState.LeaseBody(h = "zombie", e = 1L))
            client.setStored(leaseKey, ("\"stale-etag\"", payload))
          }
          _ <- IO.sleep(100.milliseconds)
          _ <- b.flatModify(state =>
            (
              state.update(
                QueueImpl.LauncherJoined(LauncherName("second"), None)
              ),
              IO.unit
            )
          )
          state <- b.get
        } yield state
      }
      .unsafeRunSync()
    finalState.knownLaunchers.keySet shouldBe Set(
      LauncherName("first"),
      LauncherName("second")
    )
    client.storedFor(leaseKey) shouldBe None
  }

  test(
    "a live lease held by someone else blocks acquirers until it is released"
  ) {
    val client = new FakeS3AsyncClient()
    val cfg = leaseConfigWith()
    val raceOnce = new java.util.concurrent.atomic.AtomicBoolean(false)

    val attemptsInBackground = new AtomicInteger(0)

    val finalState = pair(client, cfg)
      .use { case (a, b) =>
        a.flatModify(state =>
          (
            state.update(QueueImpl.LauncherJoined(LauncherName("one"), None)),
            IO.unit
          )
        ) *> IO {
          client.beforePut.set { () =>
            if (!raceOnce.getAndSet(true)) {
              val stolenPayload = com.github.plokhotnyuk.jsoniter_scala.core
                .writeToArray(
                  S3QueueState.LeaseBody(
                    h = "stranger",
                    e = System.nanoTime() + 30L * 1000L * 1000L * 1000L
                  )
                )
              client.setStored(leaseKey, ("\"stranger-etag\"", stolenPayload))
            }
          }
        } *> b
          .flatModify(state => {
            attemptsInBackground.incrementAndGet()
            (
              state.update(QueueImpl.LauncherJoined(LauncherName("two"), None)),
              IO.unit
            )
          })
          .start
          .flatMap { fiber =>
            IO.sleep(100.milliseconds) *> IO {
              client.removeStored(leaseKey)
            } *> fiber.joinWithNever
          } *> b.get
      }
      .unsafeRunSync()
    finalState.knownLaunchers.keySet shouldBe Set(
      LauncherName("one"),
      LauncherName("two")
    )
    attemptsInBackground.get should be > 0
  }

  test("a transaction without lease config uses the plain CAS path") {
    val client = new FakeS3AsyncClient()
    val finalState = tx(client, lease = None)
      .use { t =>
        t.flatModify(state =>
          (
            state.update(
              QueueImpl.LauncherJoined(LauncherName("casonly"), None)
            ),
            IO.unit
          )
        ) *> t.get
      }
      .unsafeRunSync()
    finalState.knownLaunchers.keySet shouldBe Set(LauncherName("casonly"))
    client.storedFor(leaseKey) shouldBe None
    client.deleteCount.get shouldBe 0
  }

}
