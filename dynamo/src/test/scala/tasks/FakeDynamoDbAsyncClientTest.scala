package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._
import scala.util.Try

import software.amazon.awssdk.services.dynamodb.model._

class FakeDynamoDbAsyncClientTest extends FunSuite with Matchers {

  private val tableName = "race-table"

  private def put(sk: String, ver: Long): TransactWriteItem = {
    val item = Map(
      "pk" -> AttributeValue.builder().s("tasks").build(),
      "sk" -> AttributeValue.builder().s(sk).build(),
      "ver" -> AttributeValue.builder().n(ver.toString).build()
    ).asJava
    TransactWriteItem
      .builder()
      .put(
        Put
          .builder()
          .tableName(tableName)
          .item(item)
          .conditionExpression("attribute_not_exists(#pk)")
          .expressionAttributeNames(Map("#pk" -> "pk").asJava)
          .build()
      )
      .build()
  }

  private def submit(
      client: FakeDynamoDbAsyncClient,
      item: TransactWriteItem
  ): CompletableFuture[TransactWriteItemsResponse] =
    client.transactWriteItems(
      TransactWriteItemsRequest
        .builder()
        .transactItems(List(item).asJava)
        .build()
    )

  test(
    "concurrent conflicting Puts on the same key: exactly one commits, others cancel"
  ) {
    val client = new FakeDynamoDbAsyncClient(tableName)
    val threads = 16
    val barrier = new CyclicBarrier(threads)
    val pool = Executors.newFixedThreadPool(threads)
    val successes = new AtomicInteger(0)
    val cancellations = new AtomicInteger(0)
    val others = new AtomicInteger(0)

    val tasks = (0 until threads).map { i =>
      new Runnable {
        def run(): Unit = {
          barrier.await(5, TimeUnit.SECONDS)
          Try(submit(client, put("same-sk", i.toLong + 1)).get()) match {
            case scala.util.Success(_) =>
              successes.incrementAndGet(); ()
            case scala.util.Failure(e: java.util.concurrent.ExecutionException)
                if e.getCause.isInstanceOf[TransactionCanceledException] =>
              cancellations.incrementAndGet(); ()
            case scala.util.Failure(_: TransactionCanceledException) =>
              cancellations.incrementAndGet(); ()
            case scala.util.Failure(_) =>
              others.incrementAndGet(); ()
          }
        }
      }
    }
    tasks.foreach(pool.submit)
    pool.shutdown()
    pool.awaitTermination(10, TimeUnit.SECONDS) shouldBe true

    successes.get shouldBe 1
    cancellations.get shouldBe (threads - 1)
    others.get shouldBe 0
  }

  test(
    "concurrent Puts to disjoint keys all commit without cancellation"
  ) {
    val client = new FakeDynamoDbAsyncClient(tableName)
    val threads = 16
    val barrier = new CyclicBarrier(threads)
    val pool = Executors.newFixedThreadPool(threads)
    val successes = new AtomicInteger(0)
    val cancellations = new AtomicInteger(0)

    val tasks = (0 until threads).map { i =>
      new Runnable {
        def run(): Unit = {
          barrier.await(5, TimeUnit.SECONDS)
          Try(submit(client, put(s"sk-$i", 1L)).get()) match {
            case scala.util.Success(_) =>
              successes.incrementAndGet(); ()
            case scala.util.Failure(_) =>
              cancellations.incrementAndGet(); ()
          }
        }
      }
    }
    tasks.foreach(pool.submit)
    pool.shutdown()
    pool.awaitTermination(10, TimeUnit.SECONDS) shouldBe true

    successes.get shouldBe threads
    cancellations.get shouldBe 0
    val stored = client.rawStore.count(_._1.pk == "tasks")
    stored shouldBe threads
  }

}
