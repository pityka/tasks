package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse

import tasks.queue.DynamoDb
import tasks.queue.QueueImpl
import tasks.queue.SerializableQueueState
import tasks.util.message.LauncherName

class DynamoDbTransactionTest extends FunSuite with Matchers {

  private val table = "queue"
  private val stateKey = "tasks-queue-state"
  private val partitionKey = "id"

  private class FakeDynamoDb(
      val stored: AtomicReference[Option[(Long, Array[Byte])]],
      val beforePut: AtomicReference[() => Unit] = new AtomicReference(() => ())
  ) extends DynamoDbAsyncClient {

    val putCount = new AtomicInteger(0)

    def serviceName(): String = "dynamodb"

    def close(): Unit = ()

    override def getItem(
        request: GetItemRequest
    ): CompletableFuture[GetItemResponse] = CompletableFuture.completedFuture {
      request.key.asScala(partitionKey).s shouldBe stateKey
      stored.get match {
        case None => GetItemResponse.builder().build()
        case Some((version, payload)) =>
          GetItemResponse
            .builder()
            .item(
              Map(
                partitionKey -> AttributeValue.fromS(stateKey),
                DynamoDb.versionAttribute -> AttributeValue
                  .fromN(version.toString),
                DynamoDb.stateAttribute -> AttributeValue.fromB(
                  SdkBytes.fromByteArray(payload)
                )
              ).asJava
            )
            .build()
      }
    }

    override def putItem(
        request: PutItemRequest
    ): CompletableFuture[PutItemResponse] = {
      beforePut.get.apply()
      putCount.incrementAndGet()
      val expected =
        request.expressionAttributeValues.asScala(":expected").n.toLong
      val currentVersion = stored.get.map(_._1).getOrElse(0L)
      if (expected != currentVersion)
        CompletableFuture.failedFuture(
          ConditionalCheckFailedException.builder().build()
        )
      else {
        val item = request.item.asScala
        stored.set(
          Some(
            (
              item(DynamoDb.versionAttribute).n.toLong,
              item(DynamoDb.stateAttribute).b.asByteArray()
            )
          )
        )
        CompletableFuture.completedFuture(PutItemResponse.builder().build())
      }
    }
  }

  private def transactionFor(client: DynamoDbAsyncClient) =
    DynamoDb.makeTransaction(
      client = client,
      table = table,
      stateKey = stateKey,
      partitionKeyAttribute = partitionKey,
      retryBaseDelay = 1.millisecond
    )

  private def storedState(
      client: FakeDynamoDb
  ): QueueImpl.State =
    SerializableQueueState.decode(
      DynamoDb.decompress(client.stored.get.get._2)
    )

  test("an absent item reads as the empty state") {
    val client = new FakeDynamoDb(new AtomicReference(None))
    val result = transactionFor(client).use(_.get).unsafeRunSync()
    result shouldBe QueueImpl.State.empty
  }

  test("flatModify commits the new state and bumps the version from zero") {
    val client = new FakeDynamoDb(new AtomicReference(None))

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
    client.stored.get.get._1 shouldBe 1L
    storedState(client).knownLaunchers.keySet shouldBe Set(
      LauncherName("launcher-1")
    )
  }

  test("the side effect runs only after the conditional write commits") {
    val client = new FakeDynamoDb(new AtomicReference(None))
    val sideEffectRan = new AtomicInteger(0)
    val putsAtSideEffect = new AtomicInteger(-1)

    transactionFor(client)
      .use(tx =>
        tx.flatModify(state =>
          (
            state,
            IO {
              putsAtSideEffect.set(client.putCount.get)
              sideEffectRan.incrementAndGet()
            }
          )
        )
      )
      .unsafeRunSync()

    sideEffectRan.get shouldBe 1
    putsAtSideEffect.get shouldBe 1
  }

  test("a competing commit forces a retry and the update is reapplied") {
    val client = new FakeDynamoDb(new AtomicReference(None))
    val updateInvocations = new AtomicInteger(0)

    client.beforePut.set { () =>
      if (updateInvocations.get == 1) {
        val competing = SerializableQueueState.encode(
          QueueImpl.State.empty.update(
            QueueImpl.LauncherJoined(LauncherName("competitor"), None)
          )
        )
        client.stored.set(Some((1L, DynamoDb.compress(competing))))
      }
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

    updateInvocations.get shouldBe 2
    client.stored.get.get._1 shouldBe 2L
    storedState(client).knownLaunchers.keySet shouldBe Set(
      LauncherName("competitor"),
      LauncherName("mine")
    )
  }

  test("a state larger than the item size limit fails with a legible error") {
    val client = new FakeDynamoDb(new AtomicReference(None))
    val random = new scala.util.Random(42)
    val incompressible = new Array[Byte](DynamoDb.itemSizeLimitBytes)
    random.nextBytes(incompressible)

    val error = intercept[RuntimeException] {
      transactionFor(client)
        .use(tx =>
          tx.flatModify { state =>
            (
              state.update(
                QueueImpl.RendezvousJoined(
                  tasks.util.message.RendezvousGroupId("group"),
                  0,
                  1,
                  java.util.Base64.getEncoder.encodeToString(incompressible)
                )
              ),
              IO.unit
            )
          }
        )
        .unsafeRunSync()
    }

    error.getMessage should include("does not fit in a DynamoDB item")
    error.getMessage should include(DynamoDb.itemSizeLimitBytes.toString)
    client.stored.get shouldBe None
  }

  test("compress and decompress round trip an encoded state") {
    val encoded = SerializableQueueState.encode(
      QueueImpl.State.empty.update(
        QueueImpl.Incremented(LauncherName("launcher-1"))
      )
    )
    DynamoDb.decompress(DynamoDb.compress(encoded)).toList shouldBe
      encoded.toList
  }

}
