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

  // Each load does two Query calls (tasks partition + results partition) plus
  // one GetItem for the resources singleton.
  private val queriesPerLoad = 2
  private val getItemsPerLoad = 1

  test("repeated get() calls within the TTL hit the cache (one load)") {
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
    client.queryCount.get shouldBe queriesPerLoad
    client.getItemCount.get shouldBe getItemsPerLoad
  }

  test("get() after the TTL expires re-loads") {
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
    client.queryCount.get shouldBe queriesPerLoad * 2
    client.getItemCount.get shouldBe getItemsPerLoad * 2
  }

  test("flatModify invalidates the cache so the next get() re-loads") {
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
    client.queryCount.get shouldBe queriesPerLoad * 2
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
    client.queryCount.get shouldBe queriesPerLoad
    client.transactWriteCount.get shouldBe 0
  }

}
