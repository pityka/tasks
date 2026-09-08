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
import software.amazon.awssdk.services.dynamodb.model.CancellationReason
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException

import tasks.queue.DynamoDb
import tasks.queue.QueueImpl
import tasks.queue.SerializableQueueState
import tasks.util.message.Address
import tasks.util.message.LauncherName

class DynamoDbTransactionTest extends FunSuite with Matchers {

  private val table = "queue"
  private val stateKey = "tasks-queue-state"
  private val partitionKey = "id"

  private type Store = Map[String, Map[String, AttributeValue]]

  private class FakeDynamoDb(
      val store: AtomicReference[Store],
      val beforeTransact: AtomicReference[() => Unit] = new AtomicReference(
        () => ()
      )
  ) extends DynamoDbAsyncClient {

    val transactCount = new AtomicInteger(0)

    def serviceName(): String = "dynamodb"

    def close(): Unit = ()

    override def getItem(
        request: GetItemRequest
    ): CompletableFuture[GetItemResponse] = CompletableFuture.completedFuture {
      val id = request.key.asScala(partitionKey).s
      store.get.get(id) match {
        case None       => GetItemResponse.builder().build()
        case Some(item) => GetItemResponse.builder().item(item.asJava).build()
      }
    }

    override def transactWriteItems(
        request: TransactWriteItemsRequest
    ): CompletableFuture[TransactWriteItemsResponse] = {
      beforeTransact.get.apply()
      transactCount.incrementAndGet()
      synchronized {
        val current = store.get
        val actions = request.transactItems.asScala.toList

        def conditionPasses(action: TransactWriteItem): Boolean = {
          val put = action.put
          if (put == null) true
          else if (put.conditionExpression == null) true
          else {
            val id = put.item.asScala(partitionKey).s
            val versionAttr = put.expressionAttributeNames.asScala("#version")
            val expected =
              put.expressionAttributeValues.asScala(":expected").n.toLong
            current.get(id) match {
              case None => true
              case Some(item) =>
                item.get(versionAttr).map(_.n.toLong).contains(expected)
            }
          }
        }

        val outcomes = actions.map(conditionPasses)
        if (outcomes.forall(identity)) {
          val updated = actions.foldLeft(current) { (acc, action) =>
            val put = action.put
            acc.updated(put.item.asScala(partitionKey).s, put.item.asScala.toMap)
          }
          store.set(updated)
          CompletableFuture.completedFuture(
            TransactWriteItemsResponse.builder().build()
          )
        } else {
          val reasons = outcomes.map { passed =>
            CancellationReason
              .builder()
              .code(if (passed) "None" else "ConditionalCheckFailed")
              .build()
          }
          CompletableFuture.failedFuture(
            TransactionCanceledException
              .builder()
              .cancellationReasons(reasons.asJava)
              .build()
          )
        }
      }
    }
  }

  private def transactionFor(client: DynamoDbAsyncClient, shardCount: Int) =
    DynamoDb.makeTransaction(
      client = client,
      table = table,
      stateKey = stateKey,
      partitionKeyAttribute = partitionKey,
      retryBaseDelay = 1.millisecond,
      shardCount = shardCount
    )

  private def storeOf(client: FakeDynamoDb): Store = client.store.get

  private def shardId(shard: Int): String = stateKey + "#" + shard.toString

  private def storedState(
      client: FakeDynamoDb,
      shardCount: Int
  ): QueueImpl.State = {
    val store = storeOf(client)
    store.get(stateKey) match {
      case Some(sentinel) if sentinel.contains(DynamoDb.stateAttribute) =>
        SerializableQueueState.decode(
          DynamoDb.decompress(sentinel(DynamoDb.stateAttribute).b.asByteArray())
        )
      case _ =>
        val shards = (0 until shardCount).toVector.map { shard =>
          store
            .get(shardId(shard))
            .flatMap(_.get(DynamoDb.stateAttribute))
            .map(av =>
              DynamoDb.decodeShard(DynamoDb.decompress(av.b.asByteArray()))
            )
            .getOrElse(DynamoDb.emptyShard)
        }
        DynamoDb.mergeShards(shards)
    }
  }

  private def injectCommit(
      client: FakeDynamoDb,
      state: QueueImpl.State,
      version: Long,
      shardCount: Int
  ): Unit = {
    val shards = DynamoDb.projectShards(state, shardCount)
    val sentinel = stateKey -> Map(
      partitionKey -> AttributeValue.fromS(stateKey),
      DynamoDb.versionAttribute -> AttributeValue.fromN(version.toString),
      DynamoDb.shardCountAttribute -> AttributeValue.fromN(shardCount.toString)
    )
    val shardItems = shards.zipWithIndex.map { case (shard, index) =>
      shardId(index) -> Map(
        partitionKey -> AttributeValue.fromS(shardId(index)),
        DynamoDb.stateAttribute -> AttributeValue.fromB(
          SdkBytes.fromByteArray(DynamoDb.compress(DynamoDb.encodeShard(shard)))
        )
      )
    }.toMap
    client.store.set(client.store.get ++ shardItems + sentinel)
  }

  test("an absent item reads as the empty state") {
    val client = new FakeDynamoDb(new AtomicReference[Store](Map.empty))
    val result = transactionFor(client, 4).use(_.get).unsafeRunSync()
    result shouldBe QueueImpl.State.empty
  }

  test("flatModify commits the new state and bumps the version from zero") {
    val client = new FakeDynamoDb(new AtomicReference[Store](Map.empty))

    val result = transactionFor(client, 4)
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
    storeOf(client)(stateKey)(DynamoDb.versionAttribute).n.toLong shouldBe 1L
    storedState(client, 4).knownLaunchers.keySet shouldBe Set(
      LauncherName("launcher-1")
    )
  }

  test("the side effect runs only after the transactional write commits") {
    val client = new FakeDynamoDb(new AtomicReference[Store](Map.empty))
    val sideEffectRan = new AtomicInteger(0)
    val transactsAtSideEffect = new AtomicInteger(-1)

    transactionFor(client, 4)
      .use(tx =>
        tx.flatModify(state =>
          (
            state.update(
              QueueImpl.LauncherJoined(LauncherName("launcher-1"), None)
            ),
            IO {
              transactsAtSideEffect.set(client.transactCount.get)
              sideEffectRan.incrementAndGet()
            }
          )
        )
      )
      .unsafeRunSync()

    sideEffectRan.get shouldBe 1
    transactsAtSideEffect.get shouldBe 1
  }

  test("an update that leaves the state unchanged does not write") {
    val client = new FakeDynamoDb(new AtomicReference[Store](Map.empty))
    val sideEffectRan = new AtomicInteger(0)

    val result = transactionFor(client, 4)
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
    client.transactCount.get shouldBe 0
    storeOf(client) shouldBe empty
  }

  test("a competing commit forces a retry and the update is reapplied") {
    val client = new FakeDynamoDb(new AtomicReference[Store](Map.empty))
    val updateInvocations = new AtomicInteger(0)

    client.beforeTransact.set { () =>
      if (updateInvocations.get == 1)
        injectCommit(
          client,
          QueueImpl.State.empty.update(
            QueueImpl.LauncherJoined(LauncherName("competitor"), None)
          ),
          1L,
          4
        )
    }

    transactionFor(client, 4)
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
    storeOf(client)(stateKey)(DynamoDb.versionAttribute).n.toLong shouldBe 2L
    storedState(client, 4).knownLaunchers.keySet shouldBe Set(
      LauncherName("competitor"),
      LauncherName("mine")
    )
  }

  test("a state that overflows a single item is spread across shards") {
    val shardCount = 8
    val client = new FakeDynamoDb(new AtomicReference[Store](Map.empty))
    val random = new scala.util.Random(1)
    val entries = (0 until 64).toList.map { k =>
      val bytes = new Array[Byte](16 * 1024)
      random.nextBytes(bytes)
      QueueImpl.ResultStoredForProxy(
        Address(s"proxy-$k"),
        QueueImpl.ProxyResultFailure(
          new RuntimeException(java.util.Base64.getEncoder.encodeToString(bytes))
        )
      )
    }

    transactionFor(client, shardCount)
      .use(tx =>
        tx.flatModify(state =>
          (entries.foldLeft(state)((s, e) => s.update(e)), IO.unit)
        )
      )
      .unsafeRunSync()

    val store = storeOf(client)
    val shardPayloadBytes = (0 until shardCount).flatMap { shard =>
      store
        .get(shardId(shard))
        .flatMap(_.get(DynamoDb.stateAttribute))
        .map(_.b.asByteArray().length)
    }

    shardPayloadBytes.count(_ > 0) should be > 1
    shardPayloadBytes.sum should be > DynamoDb.itemSizeLimitBytes
    shardPayloadBytes.foreach(_ should be <= DynamoDb.stateSizeLimitBytes)

    val reread = transactionFor(client, shardCount).use(_.get).unsafeRunSync()
    reread.completedResults.keySet shouldBe entries.map(_.proxy).toSet
  }

  test("a shard larger than the item size limit fails with a legible error") {
    val client = new FakeDynamoDb(new AtomicReference[Store](Map.empty))
    val random = new scala.util.Random(42)
    val incompressible = new Array[Byte](2 * DynamoDb.itemSizeLimitBytes)
    random.nextBytes(incompressible)

    val error = intercept[RuntimeException] {
      transactionFor(client, 4)
        .use(tx =>
          tx.flatModify { state =>
            (
              state.update(
                QueueImpl.MainProcessJoined(
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
    storeOf(client) shouldBe empty
  }

  test("the configured shard count must match what the table already stores") {
    val client = new FakeDynamoDb(new AtomicReference[Store](Map.empty))

    transactionFor(client, 4)
      .use(tx =>
        tx.flatModify(state =>
          (
            state.update(
              QueueImpl.LauncherJoined(LauncherName("launcher-1"), None)
            ),
            IO.unit
          )
        )
      )
      .unsafeRunSync()

    val error = intercept[RuntimeException] {
      transactionFor(client, 8).use(_.get).unsafeRunSync()
    }
    error.getMessage should include("shard")
  }

  test("a legacy single-item state is read and migrated to shards") {
    val client = new FakeDynamoDb(new AtomicReference[Store](Map.empty))
    val legacy = QueueImpl.State.empty.update(
      QueueImpl.LauncherJoined(LauncherName("legacy-launcher"), None)
    )
    client.store.set(
      Map(
        stateKey -> Map(
          partitionKey -> AttributeValue.fromS(stateKey),
          DynamoDb.versionAttribute -> AttributeValue.fromN("5"),
          DynamoDb.stateAttribute -> AttributeValue.fromB(
            SdkBytes.fromByteArray(
              DynamoDb.compress(SerializableQueueState.encode(legacy))
            )
          )
        )
      )
    )

    val readBack = transactionFor(client, 4).use(_.get).unsafeRunSync()
    readBack.knownLaunchers.keySet shouldBe Set(LauncherName("legacy-launcher"))

    transactionFor(client, 4)
      .use(tx =>
        tx.flatModify(state =>
          (
            state.update(
              QueueImpl.LauncherJoined(LauncherName("new-launcher"), None)
            ),
            IO.unit
          )
        )
      )
      .unsafeRunSync()

    val sentinel = storeOf(client)(stateKey)
    sentinel(DynamoDb.versionAttribute).n.toLong shouldBe 6L
    sentinel.contains(DynamoDb.stateAttribute) shouldBe false
    sentinel(DynamoDb.shardCountAttribute).n.toInt shouldBe 4
    storedState(client, 4).knownLaunchers.keySet shouldBe Set(
      LauncherName("legacy-launcher"),
      LauncherName("new-launcher")
    )
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
