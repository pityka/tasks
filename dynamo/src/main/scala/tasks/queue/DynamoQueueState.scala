package tasks.queue

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import cats.effect.std.Mutex
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import tasks.elastic.NodeRegistryState
import tasks.queue.QueueImpl._
import tasks.util.Transaction
import tasks.util.message.Address
import tasks.util.message.LauncherName
import tasks.util.message.MessageData.ScheduleTask
import tasks.util.message.Node

object DynamoQueueState {

  val DefaultRetryBaseDelay: FiniteDuration = 50.milliseconds
  val DefaultReadCacheTtl: FiniteDuration = 150.milliseconds

  private[tasks] val MaxOpsPerTransaction: Int = 100
  private[tasks] val MaxDataBytesPerTransaction: Int = 3 * 1024 * 1024
  private[tasks] val MaxDataOpsPerChunk: Int = 98
  private[tasks] val MaxItemBytes: Int = 380 * 1024

  final class OversizedUnitException(message: String)
      extends tasks.util.transactional.TransactionalStateDiff.OversizedUnitException(
        message
      )

  final class OversizedItemException(message: String)
      extends RuntimeException(message)

  private[tasks] val TasksPartitionKey: String = "tasks"
  private[tasks] val ResultsPartitionKey: String = "results"
  private[tasks] val ResourcesPartitionKey: String = "resources"
  private[tasks] val ResourcesSortKey: String = "singleton"

  private[tasks] val ChunkPayloadBytes: Int = 300 * 1024

  private[tasks] object Attr {
    val Pk = "pk"
    val Sk = "sk"
    val Ver = "ver"
    val Status = "status"
    val Sch = "sch"
    val Proxies = "proxies"
    val Dispatches = "dispatches"
    val PlacementValue = "placementValue"
    val ResultDelivered = "resultDelivered"
    val KnownLaunchers = "knownLaunchers"
    val Counters = "counters"
    val Nodes = "nodes"
    val CompletedResults = "completedResults"
    val MainProcesses = "mainProcesses"
    val Blob = "blob"
    val EnqueueTimestamps = "enqueueTimestamps"
    val CompletedCount = "completedCount"
    val FailedCount = "failedCount"
  }

  def clientResource(region: Region): Resource[IO, DynamoDbAsyncClient] =
    Resource.fromAutoCloseable(IO.blocking {
      DynamoDbAsyncClient
        .builder()
        .region(region)
        .credentialsProvider(DefaultCredentialsProvider.create())
        .build()
    })

  def clientResource(
      region: Region,
      endpointOverride: java.net.URI
  ): Resource[IO, DynamoDbAsyncClient] =
    Resource.fromAutoCloseable(IO.blocking {
      DynamoDbAsyncClient
        .builder()
        .region(region)
        .endpointOverride(endpointOverride)
        .credentialsProvider(DefaultCredentialsProvider.create())
        .build()
    })

  def makeTransaction(
      client: DynamoDbAsyncClient,
      table: String
  ): Resource[IO, Transaction[State]] =
    makeTransaction(
      client,
      table,
      DefaultRetryBaseDelay,
      DefaultReadCacheTtl,
      ensureTable = false
    )

  def makeTransaction(
      client: DynamoDbAsyncClient,
      table: String,
      retryBaseDelay: FiniteDuration
  ): Resource[IO, Transaction[State]] =
    makeTransaction(
      client,
      table,
      retryBaseDelay,
      DefaultReadCacheTtl,
      ensureTable = false
    )

  def makeTransaction(
      client: DynamoDbAsyncClient,
      table: String,
      retryBaseDelay: FiniteDuration,
      readCacheTtl: FiniteDuration
  ): Resource[IO, Transaction[State]] =
    makeTransaction(
      client,
      table,
      retryBaseDelay,
      readCacheTtl,
      ensureTable = false
    )

  def makeTransaction(
      client: DynamoDbAsyncClient,
      table: String,
      retryBaseDelay: FiniteDuration,
      readCacheTtl: FiniteDuration,
      ensureTable: Boolean
  ): Resource[IO, Transaction[State]] = {
    val setup: IO[Unit] =
      (if (ensureTable) ensureTableExists(client, table) else IO.unit) *>
        ensureResourcesItem(client, table)

    val resources = for {
      _ <- Resource.eval(setup)
      cached <- Resource.eval(Ref.of[IO, Option[Cached]](None))
      writeMutex <- Resource.eval(Mutex[IO])
      readMutex <- Resource.eval(Mutex[IO])
    } yield new DynamoTransaction(
      client = client,
      table = table,
      retryBaseDelay = retryBaseDelay,
      readCacheTtl = readCacheTtl,
      cached = cached,
      writeMutex = writeMutex,
      readMutex = readMutex
    )

    resources.map(tx => tx: Transaction[State])
  }

  private[tasks] case class Cached(
      state: State,
      taskVersions: Map[String, Long],
      resultChunkVersions: Map[String, Long],
      resourcesVersion: Long,
      confirmedAtNanos: Long
  )

  private[tasks] def projectionSk(p: ScheduleTaskEqualityProjection): String =
    p.description.hash

  private def ensureTableExists(
      client: DynamoDbAsyncClient,
      table: String
  ): IO[Unit] = {
    val describe = IO
      .fromCompletableFuture(
        IO(
          client.describeTable(
            DescribeTableRequest.builder().tableName(table).build()
          )
        )
      )
      .map(_ => true)
      .recover { case _: ResourceNotFoundException => false }

    val create = IO
      .fromCompletableFuture(
        IO(
          client.createTable(
            CreateTableRequest
              .builder()
              .tableName(table)
              .attributeDefinitions(
                AttributeDefinition
                  .builder()
                  .attributeName(Attr.Pk)
                  .attributeType(ScalarAttributeType.S)
                  .build(),
                AttributeDefinition
                  .builder()
                  .attributeName(Attr.Sk)
                  .attributeType(ScalarAttributeType.S)
                  .build()
              )
              .keySchema(
                KeySchemaElement
                  .builder()
                  .attributeName(Attr.Pk)
                  .keyType(KeyType.HASH)
                  .build(),
                KeySchemaElement
                  .builder()
                  .attributeName(Attr.Sk)
                  .keyType(KeyType.RANGE)
                  .build()
              )
              .billingMode(BillingMode.PAY_PER_REQUEST)
              .build()
          )
        )
      )
      .void

    val waitActive: IO[Unit] = {
      def loop: IO[Unit] =
        IO.fromCompletableFuture(
          IO(
            client.describeTable(
              DescribeTableRequest.builder().tableName(table).build()
            )
          )
        ).flatMap { response =>
          val status = response.table().tableStatus()
          if (status == TableStatus.ACTIVE) IO.unit
          else IO.sleep(200.milliseconds) *> loop
        }
      loop
    }

    describe.flatMap(exists => if (exists) IO.unit else create *> waitActive)
  }

  private def ensureResourcesItem(
      client: DynamoDbAsyncClient,
      table: String
  ): IO[Unit] = {
    val attrs = DynamoDiff
      .encodeResources(
        knownLaunchers = Map.empty,
        counters = Map.empty,
        nodes = NodeRegistryState.State.empty,
        mainProcesses = Set.empty,
        completedCount = Map.empty,
        failedCount = Map.empty,
        ver = 0L
      )
      .asJava
    val request = PutItemRequest
      .builder()
      .tableName(table)
      .item(attrs)
      .conditionExpression("attribute_not_exists(#pk)")
      .expressionAttributeNames(Map("#pk" -> Attr.Pk).asJava)
      .build()
    IO.fromCompletableFuture(IO(client.putItem(request)))
      .void
      .recover { case _: ConditionalCheckFailedException => () }
  }

  private[tasks] class DynamoTransaction(
      client: DynamoDbAsyncClient,
      table: String,
      retryBaseDelay: FiniteDuration,
      readCacheTtl: FiniteDuration,
      cached: Ref[IO, Option[Cached]],
      writeMutex: Mutex[IO],
      readMutex: Mutex[IO]
  ) extends Transaction[State] {

    private def queryPartition(
        pk: String
    ): IO[List[java.util.Map[String, AttributeValue]]] = {
      def loop(
          startKey: Option[java.util.Map[String, AttributeValue]],
          acc: List[java.util.Map[String, AttributeValue]]
      ): IO[List[java.util.Map[String, AttributeValue]]] = {
        val requestBuilder = QueryRequest
          .builder()
          .tableName(table)
          .keyConditionExpression("#pk = :pk")
          .expressionAttributeNames(Map("#pk" -> Attr.Pk).asJava)
          .expressionAttributeValues(
            Map(":pk" -> AttributeValue.builder().s(pk).build()).asJava
          )
          .consistentRead(true)
        val request = startKey match {
          case None    => requestBuilder.build()
          case Some(k) => requestBuilder.exclusiveStartKey(k).build()
        }
        IO.fromCompletableFuture(IO(client.query(request))).flatMap {
          response =>
            val items = response.items().asScala.toList
            val next = acc ++ items
            if (
              response.hasLastEvaluatedKey && !response
                .lastEvaluatedKey()
                .isEmpty
            )
              loop(Some(response.lastEvaluatedKey()), next)
            else IO.pure(next)
        }
      }
      loop(None, Nil)
    }

    private def queryAllTasks
        : IO[Map[String, (ScheduleTask, DynamoDiff.TaskItemFields, Long)]] =
      queryPartition(TasksPartitionKey).map(items =>
        items.map(DynamoDiff.decodeTaskItem).toMap
      )

    private def queryAllResultChunks: IO[Map[String, (Array[Byte], Long)]] =
      queryPartition(ResultsPartitionKey).map(items =>
        items.map(DynamoDiff.decodeResultChunkItem).toMap
      )

    private def getResources: IO[(DynamoDiff.ResourcesFields, Long)] = {
      val request = GetItemRequest
        .builder()
        .tableName(table)
        .key(
          Map(
            Attr.Pk -> AttributeValue
              .builder()
              .s(ResourcesPartitionKey)
              .build(),
            Attr.Sk -> AttributeValue.builder().s(ResourcesSortKey).build()
          ).asJava
        )
        .consistentRead(true)
        .build()
      IO.fromCompletableFuture(IO(client.getItem(request))).map { response =>
        if (response.hasItem) DynamoDiff.decodeResources(response.item())
        else
          (
            DynamoDiff.ResourcesFields(
              knownLaunchers = Map.empty,
              counters = Map.empty,
              nodes = NodeRegistryState.State.empty,
              mainProcesses = Set.empty,
              completedCount = Map.empty,
              failedCount = Map.empty
            ),
            0L
          )
      }
    }

    private def forceRead
        : IO[(State, Map[String, Long], Map[String, Long], Long)] = for {
      taskMap <- queryAllTasks
      resultChunkMap <- queryAllResultChunks
      resourcesPair <- getResources
      now <- IO.monotonic
      completedResults = DynamoDiff.assembleCompletedResults(resultChunkMap)
      state = DynamoDiff.assembleState(
        taskMap,
        completedResults,
        resourcesPair._1
      )
      taskVersions = taskMap.map { case (sk, (_, _, ver)) => sk -> ver }
      resultChunkVersions = resultChunkMap.map { case (sk, (_, ver)) =>
        sk -> ver
      }
      _ <- cached.set(
        Some(
          Cached(
            state = state,
            taskVersions = taskVersions,
            resultChunkVersions = resultChunkVersions,
            resourcesVersion = resourcesPair._2,
            confirmedAtNanos = now.toNanos
          )
        )
      )
    } yield (state, taskVersions, resultChunkVersions, resourcesPair._2)

    private def readLatest
        : IO[(State, Map[String, Long], Map[String, Long], Long)] =
      IO.monotonic.flatMap { now =>
        cached.get.flatMap {
          case Some(c)
              if now.toNanos - c.confirmedAtNanos <= readCacheTtl.toNanos =>
            IO.pure(
              (
                c.state,
                c.taskVersions,
                c.resultChunkVersions,
                c.resourcesVersion
              )
            )
          case _ =>
            readMutex.lock.surround {
              IO.monotonic.flatMap { now2 =>
                cached.get.flatMap {
                  case Some(c)
                      if now2.toNanos - c.confirmedAtNanos <= readCacheTtl.toNanos =>
                    IO.pure(
                      (
                        c.state,
                        c.taskVersions,
                        c.resultChunkVersions,
                        c.resourcesVersion
                      )
                    )
                  case _ => forceRead
                }
              }
            }
        }
      }

    override def get: IO[State] = readLatest.map(_._1)

    private def backoff(attempt: Int): IO[Unit] =
      IO.sleep(
        retryBaseDelay * math.pow(2d, math.min(attempt, 5).toDouble).toLong
      )

    private def submitTxn(
        chunk: List[TransactWriteItem]
    ): IO[Either[TransactionCanceledException, Unit]] = {
      val request = TransactWriteItemsRequest
        .builder()
        .transactItems(chunk.asJava)
        .build()
      IO.fromCompletableFuture(IO(client.transactWriteItems(request)))
        .void
        .attempt
        .flatMap {
          case Right(_)                              => IO.pure(Right(()))
          case Left(e: TransactionCanceledException) => IO.pure(Left(e))
          case Left(e)                               => IO.raiseError(e)
        }
    }

    override def flatModify[B](update: State => (State, IO[B])): IO[B] = {
      def commit(attempt: Int): IO[IO[B]] =
        readLatest.flatMap {
          case (
                baseState,
                baseTaskVersions,
                baseResultChunkVersions,
                baseResourcesVersion
              ) =>
            val (newState, sideEffect) = update(baseState)
            if (newState == baseState) IO.pure(sideEffect)
            else {
              val plan = DynamoDiff.plan(
                table = table,
                oldState = baseState,
                newState = newState,
                baseTaskVersions = baseTaskVersions,
                baseResultChunkVersions = baseResultChunkVersions,
                baseResourcesVersion = baseResourcesVersion
              )
              if (plan.chunks.isEmpty) IO.pure(sideEffect)
              else runChunks(plan.chunks, sideEffect, attempt)
            }
        }

      def runChunks(
          chunks: List[List[TransactWriteItem]],
          sideEffect: IO[B],
          attempt: Int
      ): IO[IO[B]] = chunks match {
        case Nil => IO.pure(sideEffect)
        case head :: tail =>
          submitTxn(head).flatMap {
            case Right(_) =>
              cached.set(None) *> runChunks(tail, sideEffect, attempt)
            case Left(_) =>
              cached.set(None) *> backoff(attempt) *> commit(attempt + 1)
          }
      }

      IO.uncancelable(poll => poll(writeMutex.lock.surround(commit(0))).flatten)
    }
  }

}
