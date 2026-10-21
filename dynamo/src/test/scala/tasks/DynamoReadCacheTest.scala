package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import scala.concurrent.duration._

import tasks.queue.DynamoQueueState
import tasks.queue.QueueImpl
import tasks.util.message.LauncherName

class DynamoReadCacheTest extends FunSuite with Matchers {

  private val tableName = "cache-table"

  test("repeated get() calls within the TTL hit the cache (one Query)") {
    val client = new FakeDynamoDbAsyncClient(tableName)
    DynamoQueueState
      .makeTransaction(
        client = client,
        table = tableName,
        retryBaseDelay = 1.millisecond,
        readCacheTtl = 500.milliseconds
      )
      .use { tx =>
        tx.get *> tx.get *> tx.get
      }
      .unsafeRunSync()
    client.queryCount.get shouldBe 1
    client.getItemCount.get shouldBe 1
  }

  test("get() after the TTL expires re-queries") {
    val client = new FakeDynamoDbAsyncClient(tableName)
    DynamoQueueState
      .makeTransaction(
        client = client,
        table = tableName,
        retryBaseDelay = 1.millisecond,
        readCacheTtl = 1.millisecond
      )
      .use { tx =>
        tx.get *> IO.sleep(50.milliseconds) *> tx.get
      }
      .unsafeRunSync()
    client.queryCount.get shouldBe 2
    client.getItemCount.get shouldBe 2
  }

  test("flatModify invalidates the cache so the next get() re-queries") {
    val client = new FakeDynamoDbAsyncClient(tableName)
    DynamoQueueState
      .makeTransaction(
        client = client,
        table = tableName,
        retryBaseDelay = 1.millisecond,
        readCacheTtl = 500.milliseconds
      )
      .use { tx =>
        tx.get *>
          tx.flatModify(state =>
            (
              state.update(QueueImpl.LauncherJoined(LauncherName("l-x"), None)),
              IO.unit
            )
          ) *> tx.get
      }
      .unsafeRunSync()
    client.queryCount.get shouldBe 2
  }

  test("flatModify that produces no diff does not invalidate the cache") {
    val client = new FakeDynamoDbAsyncClient(tableName)
    DynamoQueueState
      .makeTransaction(
        client = client,
        table = tableName,
        retryBaseDelay = 1.millisecond,
        readCacheTtl = 500.milliseconds
      )
      .use { tx =>
        tx.get *>
          tx.flatModify(state => (state, IO.unit)) *> tx.get
      }
      .unsafeRunSync()
    client.queryCount.get shouldBe 1
    client.transactWriteCount.get shouldBe 0
  }

}
