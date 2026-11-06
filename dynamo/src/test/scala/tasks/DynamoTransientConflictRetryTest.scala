package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.unsafe.implicits.global

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import software.amazon.awssdk.services.dynamodb.model.TransactionConflictException

import tasks.queue.DynamoQueueState

class DynamoTransientConflictRetryTest extends FunSuite with Matchers {

  private val tableName = "conflict-table"

  private def throwConflicts(times: Int, counter: AtomicInteger): () => Unit =
    () =>
      if (counter.get() < times) {
        counter.incrementAndGet()
        throw TransactionConflictException
          .builder()
          .message("Transaction is ongoing for the item")
          .build()
      }

  test("read via query retries TransactionConflictException instead of crashing") {
    val client = new FakeDynamoDbAsyncClient(tableName)
    val thrown = new AtomicInteger(0)
    client.beforeQuery.set(throwConflicts(3, thrown))

    val state = DynamoQueueState
      .makeTransaction(
        client = client,
        table = tableName,
        retryBaseDelay = 1.millisecond
      )
      .use(tx => tx.get)
      .unsafeRunSync()

    thrown.get() shouldBe 3
    state.knownLaunchers.keySet shouldBe empty
  }

  test("read via getItem retries TransactionConflictException instead of crashing") {
    val client = new FakeDynamoDbAsyncClient(tableName)
    val thrown = new AtomicInteger(0)
    client.beforeGetItem.set(throwConflicts(3, thrown))

    val state = DynamoQueueState
      .makeTransaction(
        client = client,
        table = tableName,
        retryBaseDelay = 1.millisecond
      )
      .use(tx => tx.get)
      .unsafeRunSync()

    thrown.get() shouldBe 3
    state.knownLaunchers.keySet shouldBe empty
  }

}
