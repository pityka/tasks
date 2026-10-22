package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import scala.concurrent.duration._

import tasks.queue.DynamoDiff
import tasks.queue.DynamoQueueState
import tasks.queue.Proxy
import tasks.queue.QueueImpl
import tasks.util.message.Address

object DynamoCompletedResultsTest {
  private def randomBytes(count: Int, seed: Long): Array[Byte] = {
    val rng = new scala.util.Random(seed)
    val out = new Array[Byte](count)
    rng.nextBytes(out)
    out
  }

  private def randomIso(count: Int, seed: Long): String =
    new String(
      randomBytes(count, seed),
      java.nio.charset.StandardCharsets.ISO_8859_1
    )
}
import tasks.util.message.LauncherName

class DynamoCompletedResultsTest extends FunSuite with Matchers {

  private val tableName = "completed-results-table"

  private def newClient(): FakeDynamoDbAsyncClient =
    new FakeDynamoDbAsyncClient(tableName)

  private def transactionResource(client: FakeDynamoDbAsyncClient) =
    DynamoQueueState.makeTransaction(
      client = client,
      table = tableName,
      retryBaseDelay = 1.millisecond
    )

  private def runEvents(
      client: FakeDynamoDbAsyncClient,
      events: List[QueueImpl.Event]
  ): QueueImpl.State =
    transactionResource(client)
      .use { tx =>
        events.foldLeft(IO.unit) { (acc, event) =>
          acc *> tx.flatModify(state => (state.update(event), IO.unit))
        } *> tx.get
      }
      .unsafeRunSync()

  test(
    "ResultStoredForProxy writes a per-proxy item and does not grow the resources item"
  ) {
    val client = newClient()
    val address = Address("stored-proxy")
    val result = QueueImpl.ProxyResultSuccess(
      SchedulerFixtures.untypedResult(),
      retrievedFromCache = false
    )
    val state = runEvents(
      client,
      List(QueueImpl.ResultStoredForProxy(address, result))
    )
    state.completedResults.keySet shouldBe Set(address)

    val resultItem = client.currentItem(
      DynamoQueueState.ResultsPartitionKey,
      DynamoDiff.chunkSk(address.value, 0)
    )
    resultItem shouldBe defined

    val resources = client
      .currentItem(
        DynamoQueueState.ResourcesPartitionKey,
        DynamoQueueState.ResourcesSortKey
      )
      .get
    withClue(
      "resources singleton item should not carry a completedResults attribute " +
        "so its size does not grow with task completions"
    ) {
      resources.contains(DynamoQueueState.Attr.CompletedResults) shouldBe false
    }
  }

  test(
    "many ResultStoredForProxy events do not grow the resources item past a bound"
  ) {
    val client = newClient()
    val N = 400
    val largePayloadByteCount = 1024
    val sampleBytes = Array.fill[Byte](largePayloadByteCount)(0x7a.toByte)
    val events = (0 until N).toList.map { i =>
      QueueImpl.ResultStoredForProxy(
        Address(s"bulk-$i"),
        QueueImpl.ProxyResultFailure(
          new RuntimeException(
            new String(
              sampleBytes,
              java.nio.charset.StandardCharsets.ISO_8859_1
            )
          )
        )
      )
    }
    runEvents(client, events)

    val resourceItemCount = client.rawStore.keys.count(
      _.pk ==
        DynamoQueueState.ResourcesPartitionKey
    )
    resourceItemCount shouldBe 1

    val perProxyItemCount = client.rawStore.keys.count(
      _.pk ==
        DynamoQueueState.ResultsPartitionKey
    )
    perProxyItemCount shouldBe N

    val resources = client
      .currentItem(
        DynamoQueueState.ResourcesPartitionKey,
        DynamoQueueState.ResourcesSortKey
      )
      .get
    val resourcesSize = estimateSize(resources)
    withClue(
      s"resources item size $resourcesSize bytes is dominated by $N completedResults; " +
        s"DDB's 400 KB per-item cap must not be approached by the singleton"
    ) {
      resourcesSize should be < 32768
    }
  }

  test("ResultDeliveredToProxy deletes the per-proxy item") {
    val client = newClient()
    val address = Address("delivered-proxy")
    val result = QueueImpl.ProxyResultSuccess(
      SchedulerFixtures.untypedResult(),
      retrievedFromCache = false
    )
    val state = runEvents(
      client,
      List(
        QueueImpl.ResultStoredForProxy(address, result),
        QueueImpl.ResultDeliveredToProxy(address)
      )
    )
    state.completedResults shouldBe empty

    val leftoverChunks = client.rawStore.keys.filter(k =>
      k.pk == DynamoQueueState.ResultsPartitionKey &&
        DynamoDiff.parseAddressFromSk(k.sk) == address.value
    )
    leftoverChunks shouldBe empty
  }

  test(
    "a single oversized ProxyResult spans multiple chunks under its address prefix"
  ) {
    val client = newClient()
    val address = Address("fatty")
    val bigMessage = DynamoCompletedResultsTest.randomIso(
      DynamoQueueState.ChunkPayloadBytes * 3,
      seed = 101
    )
    val result = QueueImpl.ProxyResultFailure(new RuntimeException(bigMessage))
    val state = runEvents(
      client,
      List(QueueImpl.ResultStoredForProxy(address, result))
    )
    state.completedResults.keySet shouldBe Set(address)

    val chunkKeys = client.rawStore.keys
      .filter(k =>
        k.pk == DynamoQueueState.ResultsPartitionKey &&
          DynamoDiff.parseAddressFromSk(k.sk) == address.value
      )
      .toList
    chunkKeys.size should be > 1
    val stored = state.completedResults(address)
    stored shouldBe a[QueueImpl.ProxyResultFailure]
    stored
      .asInstanceOf[QueueImpl.ProxyResultFailure]
      .cause
      .getMessage shouldBe bigMessage
  }

  test(
    "ResultDeliveredToProxy on an oversized chunked result deletes every chunk"
  ) {
    val client = newClient()
    val address = Address("fat-delivered")
    val bigMessage = new String(
      Array.fill[Byte](DynamoQueueState.ChunkPayloadBytes * 2)(0x42.toByte),
      java.nio.charset.StandardCharsets.ISO_8859_1
    )
    val result = QueueImpl.ProxyResultFailure(new RuntimeException(bigMessage))
    val state = runEvents(
      client,
      List(
        QueueImpl.ResultStoredForProxy(address, result),
        QueueImpl.ResultDeliveredToProxy(address)
      )
    )
    state.completedResults shouldBe empty
    val leftovers = client.rawStore.keys.filter(k =>
      k.pk == DynamoQueueState.ResultsPartitionKey &&
        DynamoDiff.parseAddressFromSk(k.sk) == address.value
    )
    leftovers shouldBe empty
  }

  test(
    "a single flatModify storing many large completions splits across multiple TransactWriteItems to respect the 4 MB txn cap"
  ) {
    val client = newClient()
    val count = 20
    val perResultBytes = 300 * 1024
    val events = (0 until count).toList.map { i =>
      val bigMessage = DynamoCompletedResultsTest.randomIso(
        perResultBytes,
        seed = 200L + i
      )
      QueueImpl.ResultStoredForProxy(
        Address(s"heavy-$i"),
        QueueImpl.ProxyResultFailure(new RuntimeException(bigMessage))
      )
    }
    val state = DynamoQueueState
      .makeTransaction(
        client = client,
        table = tableName,
        retryBaseDelay = 1.millisecond
      )
      .use { tx =>
        tx.flatModify(oldState =>
          (events.foldLeft(oldState)(_.update(_)), IO.unit)
        ) *> tx.get
      }
      .unsafeRunSync()
    state.completedResults.size shouldBe count
    client.transactWriteCount.get should be > 1
  }

  test(
    "a single ProxyResult whose chunks together exceed the per-txn data budget fails loudly at plan time without writing anything"
  ) {
    val client = newClient()
    val address = Address("ginormous")
    val serializedBytes =
      DynamoQueueState.MaxDataBytesPerTransaction + 512 * 1024
    val payload =
      DynamoCompletedResultsTest.randomIso(serializedBytes, seed = 300)
    val result = QueueImpl.ProxyResultFailure(new RuntimeException(payload))
    val outcome = DynamoQueueState
      .makeTransaction(
        client = client,
        table = tableName,
        retryBaseDelay = 1.millisecond
      )
      .use { tx =>
        tx.flatModify(state =>
          (
            state.update(QueueImpl.ResultStoredForProxy(address, result)),
            IO.unit
          )
        )
      }
      .attempt
      .unsafeRunSync()
    val error = outcome.left.toOption.getOrElse(fail("expected plan to fail"))
    error shouldBe a[DynamoQueueState.OversizedUnitException]
    val msg = error.getMessage
    msg should include("ginormous")
    msg should include("SharedFile")
    msg should include("ProxyResultFailure")
    msg should include("RuntimeException")
    msg should include("messageLength=")
    msg should include("messagePrefix=")
    msg should include("stackHead=")

    val resultChunks =
      client.rawStore.keys.count(_.pk == DynamoQueueState.ResultsPartitionKey)
    resultChunks shouldBe 0
  }

  test(
    "oversized ProxyResultSuccess diagnostic names files, inline length, and payload prefix"
  ) {
    val client = newClient()
    val address = Address("plus-size-success")
    val huge = DynamoCompletedResultsTest.randomIso(
      DynamoQueueState.MaxDataBytesPerTransaction + 512 * 1024,
      seed = 400
    )
    val result = QueueImpl.ProxyResultSuccess(
      tasks.queue.UntypedResult(
        files = Set.empty,
        data = tasks.queue.Base64Data(huge),
        mutableFiles = None
      ),
      retrievedFromCache = false
    )
    val outcome = DynamoQueueState
      .makeTransaction(
        client = client,
        table = tableName,
        retryBaseDelay = 1.millisecond
      )
      .use { tx =>
        tx.flatModify(state =>
          (
            state.update(QueueImpl.ResultStoredForProxy(address, result)),
            IO.unit
          )
        )
      }
      .attempt
      .unsafeRunSync()
    val error = outcome.left.toOption.getOrElse(fail("expected plan to fail"))
    val msg = error.getMessage
    msg should include("plus-size-success")
    msg should include("ProxyResultSuccess")
    msg should include("files=0")
    msg should include("inlineBase64Length=")
    msg should include("inlineBase64Prefix=")
  }

  test(
    "oversized ProxyResult names the originating task when the proxy is still on a scheduled task"
  ) {
    val client = newClient()
    val launcher = LauncherName("l-hint")
    val allocated = SchedulerFixtures.allocated()
    val sch = SchedulerFixtures.scheduleTask("hinted-task", "hinted-hash")
    val address = Address("hintedproxy")
    val proxy = Proxy(address)
    val huge = DynamoCompletedResultsTest.randomIso(
      DynamoQueueState.MaxDataBytesPerTransaction + 256 * 1024,
      seed = 500
    )
    val result = QueueImpl.ProxyResultFailure(new RuntimeException(huge))
    val outcome = DynamoQueueState
      .makeTransaction(
        client = client,
        table = tableName,
        retryBaseDelay = 1.millisecond
      )
      .use { tx =>
        tx.flatModify(state =>
          (state.update(QueueImpl.Enqueued(sch, List(proxy), 0L)), IO.unit)
        ) *>
          tx.flatModify(state =>
            (
              state.update(
                QueueImpl.TaskScheduled(sch, launcher, allocated, None)
              ),
              IO.unit
            )
          ) *>
          tx.flatModify(state =>
            (
              state.update(QueueImpl.ResultStoredForProxy(address, result)),
              IO.unit
            )
          )
      }
      .attempt
      .unsafeRunSync()
    val error = outcome.left.toOption.getOrElse(fail("expected plan to fail"))
    val msg = error.getMessage
    msg should include("hintedproxy")
    msg should include("id=hinted-task")
    msg should include("dataHash=hinted-hash")
  }

  test(
    "SessionProxiesDropped sweeps per-proxy result items for addresses in the dead session"
  ) {
    val client = newClient()
    val deadSession = "ghost"
    val deadTag = tasks.util.SessionId.tag(
      deadSession,
      "Launcher-SimpleSocketAddress(localhost,9999)"
    )
    val deadAddr = Address(deadTag)
    val liveAddr = Address("alive")
    val result = QueueImpl.ProxyResultSuccess(
      SchedulerFixtures.untypedResult(),
      retrievedFromCache = false
    )
    val state = runEvents(
      client,
      List(
        QueueImpl.ResultStoredForProxy(deadAddr, result),
        QueueImpl.ResultStoredForProxy(liveAddr, result),
        QueueImpl.SessionProxiesDropped(deadSession)
      )
    )
    state.completedResults.keySet shouldBe Set(liveAddr)

    val remainingAddresses = client.rawStore.keys
      .filter(_.pk == DynamoQueueState.ResultsPartitionKey)
      .map(k => DynamoDiff.parseAddressFromSk(k.sk))
      .toSet
    remainingAddresses shouldBe Set(liveAddr.value)
  }

  private def estimateSize(
      attrs: Map[
        String,
        software.amazon.awssdk.services.dynamodb.model.AttributeValue
      ]
  ): Int = {
    import software.amazon.awssdk.services.dynamodb.model.AttributeValue
    def one(av: AttributeValue): Int = {
      var size = 0
      if (av.s() != null) size += av.s().getBytes("UTF-8").length
      if (av.n() != null) size += av.n().getBytes("UTF-8").length
      if (av.b() != null) size += av.b().asByteArray().length
      if (av.bool() != null) size += 1
      if (av.hasL) {
        import scala.jdk.CollectionConverters._
        av.l().asScala.foreach(x => size += one(x))
      }
      if (av.hasM) {
        import scala.jdk.CollectionConverters._
        av.m().asScala.foreach { case (k, v) =>
          size += k.getBytes("UTF-8").length
          size += one(v)
        }
      }
      if (av.hasSs) {
        import scala.jdk.CollectionConverters._
        av.ss().asScala.foreach(s => size += s.getBytes("UTF-8").length)
      }
      size
    }
    attrs.foldLeft(0) { case (acc, (k, v)) =>
      acc + k.getBytes("UTF-8").length + one(v)
    }
  }

}
