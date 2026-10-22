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

  private def randomBytes(count: Int, seed: Long): Array[Byte] = {
    val rng = new scala.util.Random(seed)
    val out = new Array[Byte](count)
    rng.nextBytes(out)
    out
  }

  test(
    "a ScheduleTask whose compressed form still exceeds the DDB per-item limit fails loudly at plan time without writing anything"
  ) {
    val client = newClient()
    val incompressible = new String(
      randomBytes(DynamoQueueState.MaxItemBytes + 64 * 1024, seed = 42),
      java.nio.charset.StandardCharsets.ISO_8859_1
    )
    val sch = SchedulerFixtures.scheduleTask(
      "fat-task",
      "fat-hash",
      inlineInput = incompressible
    )
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
    msg should include("compressedScheduleTaskBytes=")
    msg should include("after gzip")

    val taskItemCount =
      client.rawStore.keys.count(_.pk == DynamoQueueState.TasksPartitionKey)
    taskItemCount shouldBe 0
  }

  test(
    "a large but highly compressible ScheduleTask input is persisted after gzip (no OversizedItemException)"
  ) {
    val client = newClient()
    // Simulates a user payload that is big as JSON but compresses heavily —
    // e.g. a long list of SharedFile handles with repetitive key names.
    val repetitive =
      ("{\"name\":\"/data/file-abcdefghij\",\"hash\":\"sha256-abcdefghij\",\"size\":12345}," * 20000)
    val sch = SchedulerFixtures.scheduleTask(
      "big-input-task",
      "bit-hash",
      inlineInput = repetitive
    )
    val proxy = Proxy(Address("big-proxy"))

    val state = DynamoQueueState
      .makeTransaction(
        client = client,
        table = tableName,
        retryBaseDelay = 1.millisecond
      )
      .use { tx =>
        tx.flatModify(state =>
          (state.update(QueueImpl.Enqueued(sch, List(proxy), 0L)), IO.unit)
        ) *> tx.get
      }
      .unsafeRunSync()

    state.queuedTasks.size shouldBe 1
    val readBack = state.queuedTasks.values.head._1
    readBack.input.b64.value shouldBe repetitive
    val storedBlob = client
      .currentItem(DynamoQueueState.TasksPartitionKey, sch.description.hash)
      .get(DynamoQueueState.Attr.Sch)
      .b()
      .asByteArray()
      .length
    storedBlob should be < DynamoQueueState.MaxItemBytes
    repetitive.length should be > DynamoQueueState.MaxItemBytes
  }

}
