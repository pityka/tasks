package tasks.queue

import cats.effect.IO
import cats.effect.kernel.Resource

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.Put
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException

import com.github.plokhotnyuk.jsoniter_scala.core._

import tasks.elastic.NodeRegistryState
import tasks.queue.QueueImpl.State

object DynamoDb {

  val itemSizeLimitBytes = 400 * 1024

  private val itemOverheadAllowanceBytes = 2 * 1024

  val stateSizeLimitBytes = itemSizeLimitBytes - itemOverheadAllowanceBytes

  val transactionSizeLimitBytes = 4 * 1024 * 1024 - 64 * 1024

  val transactionItemLimit = 100

  val versionAttribute = "version"

  val stateAttribute = "state"

  val shardCountAttribute = "shards"

  val DefaultStateKey = "tasks-queue-state"

  val DefaultPartitionKeyAttribute = "id"

  val DefaultRetryBaseDelay = 20.milliseconds

  val DefaultShardCount = 32

  def clientResource(
      region: Option[String]
  ): Resource[IO, DynamoDbAsyncClient] =
    Resource.fromAutoCloseable(IO.blocking {
      val builder = DynamoDbAsyncClient.builder()
      region
        .map(r => builder.region(Region.of(r)))
        .getOrElse(builder)
        .build()
    })

  def makeTransaction(
      table: String,
      region: Option[String]
  ): Resource[IO, tasks.util.Transaction[State]] =
    makeTransaction(
      table = table,
      region = region,
      stateKey = DefaultStateKey,
      partitionKeyAttribute = DefaultPartitionKeyAttribute,
      retryBaseDelay = DefaultRetryBaseDelay,
      shardCount = DefaultShardCount
    )

  def makeTransaction(
      table: String,
      region: Option[String],
      stateKey: String,
      partitionKeyAttribute: String,
      retryBaseDelay: FiniteDuration,
      shardCount: Int
  ): Resource[IO, tasks.util.Transaction[State]] =
    clientResource(region).flatMap(client =>
      makeTransaction(
        client = client,
        table = table,
        stateKey = stateKey,
        partitionKeyAttribute = partitionKeyAttribute,
        retryBaseDelay = retryBaseDelay,
        shardCount = shardCount
      )
    )

  def makeTransaction(
      client: DynamoDbAsyncClient,
      table: String,
      stateKey: String,
      partitionKeyAttribute: String,
      retryBaseDelay: FiniteDuration,
      shardCount: Int
  ): Resource[IO, tasks.util.Transaction[State]] =
    Resource.eval(
      IO(
        new DynamoDbTransaction(
          client = client,
          table = table,
          stateKey = stateKey,
          partitionKeyAttribute = partitionKeyAttribute,
          retryBaseDelay = retryBaseDelay,
          shardCount = shardCount
        )
      )
    )

  private[tasks] def compress(bytes: Array[Byte]): Array[Byte] = {
    val out = new java.io.ByteArrayOutputStream()
    val gzip = new java.util.zip.GZIPOutputStream(out)
    try gzip.write(bytes)
    finally gzip.close()
    out.toByteArray
  }

  private[tasks] def decompress(bytes: Array[Byte]): Array[Byte] = {
    val gzip = new java.util.zip.GZIPInputStream(
      new java.io.ByteArrayInputStream(bytes)
    )
    try gzip.readAllBytes()
    finally gzip.close()
  }

  private val readerConfig = ReaderConfig
    .withMaxBufSize(2147483645)
    .withMaxCharBufSize(2147483645)

  private[tasks] val emptyShard: SerializableQueueState =
    SerializableQueueState(
      queuedTasks = Nil,
      scheduledTasks = Nil,
      knownLaunchers = Nil,
      counters = Nil,
      nodes = NodeRegistryState.State.empty,
      completedResults = Nil,
      mainProcesses = Nil
    )

  private[tasks] def encodeShard(shard: SerializableQueueState): Array[Byte] =
    writeToArray(shard)(SerializableQueueState.codec)

  private[tasks] def decodeShard(bytes: Array[Byte]): SerializableQueueState =
    readFromArray[SerializableQueueState](bytes, readerConfig)(
      SerializableQueueState.codec
    )

  private def bucketOf(hash: Int, shardCount: Int): Int =
    java.lang.Math.floorMod(hash, shardCount)

  private[tasks] def projectShards(
      state: State,
      shardCount: Int
  ): Vector[SerializableQueueState] = {
    val queued = state.queuedTasks.groupBy { case (key, _) =>
      bucketOf(key.hashCode, shardCount)
    }
    val scheduled = state.scheduledTasks.groupBy { case (key, _) =>
      bucketOf(key.hashCode, shardCount)
    }
    val launchers = state.knownLaunchers.groupBy { case (key, _) =>
      bucketOf(key.hashCode, shardCount)
    }
    val counters = state.counters.groupBy { case (key, _) =>
      bucketOf(key.hashCode, shardCount)
    }
    val results = state.completedResults.groupBy { case (key, _) =>
      bucketOf(key.hashCode, shardCount)
    }
    (0 until shardCount).toVector.map { shard =>
      SerializableQueueState(
        queuedTasks = queued.getOrElse(shard, Map.empty).toList,
        scheduledTasks = scheduled.getOrElse(shard, Map.empty).toList,
        knownLaunchers = launchers.getOrElse(shard, Map.empty).toList,
        counters = counters.getOrElse(shard, Map.empty).toList,
        nodes = if (shard == 0) state.nodes else NodeRegistryState.State.empty,
        completedResults = results.getOrElse(shard, Map.empty).toList,
        mainProcesses =
          if (shard == 0) state.mainProcesses.toList.sorted else Nil
      )
    }
  }

  private[tasks] def mergeShards(
      shards: Vector[SerializableQueueState]
  ): State =
    State(
      queuedTasks = shards.iterator.flatMap(_.queuedTasks).toMap,
      scheduledTasks = shards.iterator.flatMap(_.scheduledTasks).toMap,
      knownLaunchers = shards.iterator.flatMap(_.knownLaunchers).toMap,
      counters = shards.iterator.flatMap(_.counters).toMap,
      nodes =
        shards.headOption.map(_.nodes).getOrElse(NodeRegistryState.State.empty),
      completedResults = shards.iterator.flatMap(_.completedResults).toMap,
      mainProcesses = shards.iterator.flatMap(_.mainProcesses).toSet
    )

  private def sameShardContent(
      a: SerializableQueueState,
      b: SerializableQueueState
  ): Boolean =
    a.queuedTasks.toMap == b.queuedTasks.toMap &&
      a.scheduledTasks.toMap == b.scheduledTasks.toMap &&
      a.knownLaunchers.toMap == b.knownLaunchers.toMap &&
      a.counters.toMap == b.counters.toMap &&
      a.completedResults.toMap == b.completedResults.toMap &&
      a.nodes == b.nodes &&
      a.mainProcesses.toSet == b.mainProcesses.toSet

  private def describeShardEntries(
      shard: SerializableQueueState
  ): List[(String, Int)] = {
    def sizeOf(single: SerializableQueueState): Int =
      compress(encodeShard(single)).length
    shard.queuedTasks.map { entry =>
      val description = entry._1.description
      (
        s"queued task ${description.taskId.id}.${description.taskId.version} (dataHash=${description.dataHash})",
        sizeOf(emptyShard.copy(queuedTasks = List(entry)))
      )
    } ++ shard.scheduledTasks.map { entry =>
      val description = entry._1.description
      (
        s"scheduled task ${description.taskId.id}.${description.taskId.version} (dataHash=${description.dataHash})",
        sizeOf(emptyShard.copy(scheduledTasks = List(entry)))
      )
    } ++ shard.completedResults.map { entry =>
      (
        s"completed result for proxy ${entry._1.value}",
        sizeOf(emptyShard.copy(completedResults = List(entry)))
      )
    } ++ shard.knownLaunchers.map { entry =>
      (
        s"launcher ${entry._1.name}",
        sizeOf(emptyShard.copy(knownLaunchers = List(entry)))
      )
    } ++ shard.mainProcesses.map { session =>
      (
        s"main process session $session",
        sizeOf(emptyShard.copy(mainProcesses = List(session)))
      )
    }
  }

  private def shardContentSummary(shard: SerializableQueueState): String =
    s"queued=${shard.queuedTasks.size} scheduled=${shard.scheduledTasks.size} " +
      s"completedResults=${shard.completedResults.size} knownLaunchers=${shard.knownLaunchers.size} " +
      s"counters=${shard.counters.size} mainProcesses=${shard.mainProcesses.size}"

  private def oversizedShardMessage(
      shardIndex: Int,
      shard: SerializableQueueState,
      compressedLength: Int,
      effectiveShards: Int
  ): String = {
    val entries = describeShardEntries(shard)
    val largest = if (entries.isEmpty) None else Some(entries.maxBy(_._2))
    val advice = largest match {
      case Some((description, size)) if size > stateSizeLimitBytes =>
        s"Its largest single entry, $description, is ~$size compressed bytes on its own, over the per-item limit. " +
          "A single entry cannot be split across shards: move its payload out of the task input or result " +
          "(pass large data as a SharedFile reference), or use a queue state backend without a per-item size limit."
      case Some((description, size)) =>
        s"Its largest single entry, $description, is ~$size compressed bytes; no single entry is over the limit, so the " +
          s"shard overflows because its entries sum past it. Increasing the shard count (currently $effectiveShards) " +
          "would spread them across more items."
      case None =>
        s"Increase the shard count (currently $effectiveShards) so the state spreads across more items, or use a " +
          "queue state backend without a per-item size limit."
    }
    s"Queue state shard $shardIndex does not fit in a DynamoDB item: $compressedLength compressed bytes exceeds the " +
      s"usable per-item limit of $stateSizeLimitBytes (DynamoDB caps an item at $itemSizeLimitBytes). " +
      s"Shard holds: ${shardContentSummary(shard)}. $advice"
  }

  private case class Sentinel(
      version: Long,
      shardCount: Option[Int],
      legacyState: Option[State]
  )

  private[tasks] class DynamoDbTransaction(
      client: DynamoDbAsyncClient,
      table: String,
      stateKey: String,
      partitionKeyAttribute: String,
      retryBaseDelay: FiniteDuration,
      shardCount: Int
  ) extends tasks.util.Transaction[State] {

    require(
      shardCount >= 1 && shardCount < transactionItemLimit,
      s"shardCount must be between 1 and ${transactionItemLimit - 1}, was $shardCount"
    )

    private def sentinelId = stateKey

    private def shardId(shard: Int) = stateKey + "#" + shard.toString

    private def keyOf(id: String) =
      Map(partitionKeyAttribute -> AttributeValue.fromS(id)).asJava

    private def missingTable(e: ResourceNotFoundException) =
      new RuntimeException(
        s"DynamoDB table '$table' not found. It must exist before the task system starts, " +
          s"with a single String partition key named '$partitionKeyAttribute' and no sort key.",
        e
      )

    private def getItem(id: String): IO[Option[Map[String, AttributeValue]]] = {
      val request = GetItemRequest
        .builder()
        .tableName(table)
        .key(keyOf(id))
        .consistentRead(true)
        .build()

      IO.fromCompletableFuture(IO(client.getItem(request)))
        .adaptError { case e: ResourceNotFoundException => missingTable(e) }
        .map { response =>
          if (!response.hasItem) None
          else Some(response.item.asScala.toMap)
        }
    }

    private def readSentinel: IO[Sentinel] =
      getItem(sentinelId).map {
        case None => Sentinel(0L, None, None)
        case Some(item) =>
          val version = item.get(versionAttribute).map(_.n.toLong).getOrElse(0L)
          val storedShardCount =
            item.get(shardCountAttribute).map(_.n.toInt)
          val legacyState = item
            .get(stateAttribute)
            .map(attribute =>
              SerializableQueueState.decode(
                decompress(attribute.b.asByteArray())
              )
            )
          Sentinel(version, storedShardCount, legacyState)
      }

    private def readShard(shard: Int): IO[SerializableQueueState] =
      getItem(shardId(shard)).map {
        case None => emptyShard
        case Some(item) =>
          item
            .get(stateAttribute)
            .map(attribute => decodeShard(decompress(attribute.b.asByteArray())))
            .getOrElse(emptyShard)
      }

    private def effectiveShardCount(stored: Option[Int]): IO[Int] =
      stored match {
        case Some(n) if n != shardCount =>
          IO.raiseError(
            new RuntimeException(
              s"DynamoDB table '$table' stores the queue state across $n shards, but this process " +
                s"is configured for $shardCount. The shard count is fixed once the table holds state " +
                "and must be identical across every process sharing the table."
            )
          )
        case Some(n) => IO.pure(n)
        case None    => IO.pure(shardCount)
      }

    private def readSnapshot
        : IO[(State, Long, Int, Vector[SerializableQueueState])] =
      readSentinel.flatMap { sentinel =>
        sentinel.legacyState match {
          case Some(legacy) =>
            IO.pure(
              (
                legacy,
                sentinel.version,
                shardCount,
                Vector.fill(shardCount)(emptyShard)
              )
            )
          case None =>
            effectiveShardCount(sentinel.shardCount).flatMap { n =>
              IO.parSequenceN(math.min(n, 16))(
                (0 until n).toList.map(shard => readShard(shard))
              ).map { shards =>
                val stored = shards.toVector
                (mergeShards(stored), sentinel.version, n, stored)
              }
            }
        }
      }

    private def writeShardsAndBump(
        expectedVersion: Long,
        effectiveShards: Int,
        changed: List[(Int, Array[Byte])]
    ): IO[Boolean] = {
      val sentinelPut = TransactWriteItem
        .builder()
        .put(
          Put
            .builder()
            .tableName(table)
            .item(
              Map(
                partitionKeyAttribute -> AttributeValue.fromS(sentinelId),
                versionAttribute -> AttributeValue
                  .fromN((expectedVersion + 1L).toString),
                shardCountAttribute -> AttributeValue
                  .fromN(effectiveShards.toString)
              ).asJava
            )
            .conditionExpression(
              "attribute_not_exists(#version) OR #version = :expected"
            )
            .expressionAttributeNames(
              Map("#version" -> versionAttribute).asJava
            )
            .expressionAttributeValues(
              Map(
                ":expected" -> AttributeValue.fromN(expectedVersion.toString)
              ).asJava
            )
            .build()
        )
        .build()

      val shardPuts = changed.map { case (shard, payload) =>
        TransactWriteItem
          .builder()
          .put(
            Put
              .builder()
              .tableName(table)
              .item(
                Map(
                  partitionKeyAttribute -> AttributeValue.fromS(shardId(shard)),
                  stateAttribute -> AttributeValue.fromB(
                    SdkBytes.fromByteArray(payload)
                  )
                ).asJava
              )
              .build()
          )
          .build()
      }

      val request = TransactWriteItemsRequest
        .builder()
        .transactItems((sentinelPut :: shardPuts).asJava)
        .build()

      IO.fromCompletableFuture(IO(client.transactWriteItems(request)))
        .as(true)
        .recoverWith { case e: TransactionCanceledException =>
          val reasons = Option(e.cancellationReasons())
            .map(_.asScala.toList)
            .getOrElse(Nil)
            .flatMap(reason => Option(reason.code()))
          if (
            reasons.exists(code =>
              code == "ConditionalCheckFailed" || code == "TransactionConflict"
            )
          ) IO.pure(false)
          else IO.raiseError(e)
        }
        .adaptError { case e: ResourceNotFoundException => missingTable(e) }
    }

    private def backoff(attempt: Int): IO[Unit] =
      IO.sleep(
        retryBaseDelay * math.pow(2d, math.min(attempt, 5).toDouble).toLong
      )

    override def flatModify[B](update: State => (State, IO[B])): IO[B] = {
      def loop(attempt: Int): IO[IO[B]] =
        readSnapshot.flatMap { case (state, version, effectiveShards, stored) =>
          val (updated, sideEffect) = update(state)
          if (updated == state)
            IO(
              scribe.trace(
                "Queue state unchanged by this update, skipping the write."
              )
            ).as(sideEffect)
          else {
            val projected = projectShards(updated, effectiveShards)
            val changed = (0 until effectiveShards).toList.flatMap { shard =>
              if (sameShardContent(projected(shard), stored(shard))) None
              else Some(shard -> compress(encodeShard(projected(shard))))
            }
            val oversizedShard = changed.find { case (_, payload) =>
              payload.length > stateSizeLimitBytes
            }
            val changedBytes = changed.map { case (_, payload) =>
              payload.length.toLong
            }.sum
            oversizedShard match {
              case Some((shardIndex, payload)) =>
                IO.raiseError(
                  new RuntimeException(
                    oversizedShardMessage(
                      shardIndex,
                      projected(shardIndex),
                      payload.length,
                      effectiveShards
                    )
                  )
                )
              case None if changedBytes > transactionSizeLimitBytes =>
                IO.raiseError(
                  new RuntimeException(
                    s"This queue state update rewrites $changedBytes compressed bytes across " +
                      s"${changed.size} shards in a single transaction, exceeding the DynamoDB " +
                      s"transaction limit of $transactionSizeLimitBytes usable bytes. A single atomic " +
                      "mutation cannot rewrite this much state at once; increase the shard count so each " +
                      "shard is smaller, or use a queue state backend without a per-transaction size limit."
                  )
                )
              case None =>
                writeShardsAndBump(version, effectiveShards, changed).flatMap {
                  committed =>
                    if (committed) IO.pure(sideEffect)
                    else
                      IO(
                        scribe.debug(
                          "Conditional write of the queue state failed because another process committed first. Try again.",
                          scribe.data(
                            Map(
                              "expected-version" -> version,
                              "attempt" -> attempt
                            )
                          )
                        )
                      ) *> backoff(attempt) *> loop(attempt + 1)
                }
            }
          }
        }

      IO.uncancelable { poll =>
        poll(loop(0)).flatten
      }
    }

    override def get: IO[State] = readSnapshot.map(_._1)

  }

}
