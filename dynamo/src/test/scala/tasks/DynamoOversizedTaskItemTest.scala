package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import scala.concurrent.duration._

import tasks.queue.DynamoQueueState
import tasks.queue.Proxy
import tasks.queue.QueueImpl
import tasks.util.message.Address

class DynamoOversizedTaskItemTest extends FunSuite with Matchers {

  private val tableName = "oversized-item-table"

  private def newClient(): FakeDynamoDbAsyncClient =
    new FakeDynamoDbAsyncClient(tableName)

  test(
    "a ScheduleTask whose serialized form exceeds the DDB per-item limit fails loudly at plan time without writing anything"
  ) {
    val client = newClient()
    val bigInline = new String(
      Array.fill[Byte](DynamoQueueState.MaxItemBytes + 32 * 1024)(0x51.toByte),
      java.nio.charset.StandardCharsets.ISO_8859_1
    )
    val sch =
      SchedulerFixtures.scheduleTask("fat-task", "fat-hash", inlineInput = bigInline)
    val proxy = Proxy(Address("fat-proxy"))

    val outcome = DynamoQueueState
      .makeTransaction(
        client = client,
        table = tableName,
        retryBaseDelay = 1.millisecond
      )
      .use { tx =>
        tx.flatModify(state =>
          (state.update(QueueImpl.Enqueued(sch, List(proxy), 0L)), IO.unit)
        )
      }
      .attempt
      .unsafeRunSync()

    val error = outcome.left.toOption.getOrElse(fail("expected plan to fail"))
    error shouldBe a[DynamoQueueState.OversizedItemException]
    val msg = error.getMessage
    msg should include("fat-task")
    msg should include("fat-hash")
    msg should include("SharedFile")
    msg should include("inputBase64Length=")

    val taskItemCount =
      client.rawStore.keys.count(_.pk == DynamoQueueState.TasksPartitionKey)
    taskItemCount shouldBe 0
  }

}
