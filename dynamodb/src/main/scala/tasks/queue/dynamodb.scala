package tasks.queue

import cats.effect.IO
import cats.effect.kernel.Resource

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException

import tasks.queue.QueueImpl.State

object DynamoDb {

  val itemSizeLimitBytes = 400 * 1024

  private val itemOverheadAllowanceBytes = 2 * 1024

  val stateSizeLimitBytes = itemSizeLimitBytes - itemOverheadAllowanceBytes

  val versionAttribute = "version"

  val stateAttribute = "state"

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
      region: Option[String],
      stateKey: String = "tasks-queue-state",
      partitionKeyAttribute: String = "id",
      retryBaseDelay: FiniteDuration = 20.milliseconds
  ): Resource[IO, tasks.util.Transaction[State]] =
    clientResource(region).flatMap(client =>
      makeTransaction(
        client = client,
        table = table,
        stateKey = stateKey,
        partitionKeyAttribute = partitionKeyAttribute,
        retryBaseDelay = retryBaseDelay
      )
    )

  def makeTransaction(
      client: DynamoDbAsyncClient,
      table: String,
      stateKey: String,
      partitionKeyAttribute: String,
      retryBaseDelay: FiniteDuration
  ): Resource[IO, tasks.util.Transaction[State]] =
    Resource.eval(
      IO.pure(
        new DynamoDbTransaction(
          client = client,
          table = table,
          stateKey = stateKey,
          partitionKeyAttribute = partitionKeyAttribute,
          retryBaseDelay = retryBaseDelay
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

  private[tasks] class DynamoDbTransaction(
      client: DynamoDbAsyncClient,
      table: String,
      stateKey: String,
      partitionKeyAttribute: String,
      retryBaseDelay: FiniteDuration
  ) extends tasks.util.Transaction[State] {

    private val keyAttributes = Map(
      partitionKeyAttribute -> AttributeValue.fromS(stateKey)
    ).asJava

    private def missingTable(e: ResourceNotFoundException) =
      new RuntimeException(
        s"DynamoDB table '$table' not found. It must exist before the task system starts, " +
          s"with a single String partition key named '$partitionKeyAttribute' and no sort key.",
        e
      )

    private def readVersionedState: IO[(State, Long)] = {
      val request = GetItemRequest
        .builder()
        .tableName(table)
        .key(keyAttributes)
        .consistentRead(true)
        .build()

      IO.fromCompletableFuture(IO(client.getItem(request)))
        .adaptError { case e: ResourceNotFoundException => missingTable(e) }
        .map { response =>
          if (!response.hasItem) (State.empty, 0L)
          else {
            val item = response.item.asScala
            val version =
              item.get(versionAttribute).map(_.n.toLong).getOrElse(0L)
            val state = item
              .get(stateAttribute)
              .map(attribute =>
                SerializableQueueState.decode(
                  decompress(attribute.b.asByteArray())
                )
              )
              .getOrElse(State.empty)
            (state, version)
          }
        }
    }

    private def writeIfUnchanged(
        state: State,
        expectedVersion: Long
    ): IO[Boolean] =
      IO(compress(SerializableQueueState.encode(state))).flatMap { payload =>
        if (payload.length > stateSizeLimitBytes)
          IO.raiseError(
            new RuntimeException(
              s"The queue state does not fit in a DynamoDB item: ${payload.length} compressed bytes " +
                s"exceeds the usable limit of $stateSizeLimitBytes (DynamoDB caps an item at $itemSizeLimitBytes). " +
                "Reduce the number of tasks in flight, or use a queue state backend without a per-item size limit."
            )
          )
        else {
          val request = PutItemRequest
            .builder()
            .tableName(table)
            .item(
              (Map(
                partitionKeyAttribute -> AttributeValue.fromS(stateKey),
                versionAttribute -> AttributeValue
                  .fromN((expectedVersion + 1L).toString),
                stateAttribute -> AttributeValue.fromB(
                  SdkBytes.fromByteArray(payload)
                )
              )).asJava
            )
            .conditionExpression(
              s"attribute_not_exists(#version) OR #version = :expected"
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

          IO.fromCompletableFuture(IO(client.putItem(request)))
            .as(true)
            .recover { case _: ConditionalCheckFailedException => false }
            .adaptError { case e: ResourceNotFoundException => missingTable(e) }
        }
      }

    private def backoff(attempt: Int): IO[Unit] =
      IO.sleep(
        retryBaseDelay * math.pow(2d, math.min(attempt, 5).toDouble).toLong
      )

    override def flatModify[B](update: State => (State, IO[B])): IO[B] = {
      def loop(attempt: Int): IO[IO[B]] =
        readVersionedState.flatMap { case (state, version) =>
          val (updated, sideEffect) = update(state)
          if (updated == state)
            IO(
              scribe.trace(
                "Queue state unchanged by this update, skipping the write."
              )
            ).as(sideEffect)
          else
            writeIfUnchanged(updated, version).flatMap { committed =>
              if (committed) IO.pure(sideEffect)
              else
                IO(
                  scribe.debug(
                    "Conditional write of the queue state failed because another process committed first. Try again.",
                    scribe.data(
                      Map("expected-version" -> version, "attempt" -> attempt)
                    )
                  )
                ) *> backoff(attempt) *> loop(attempt + 1)
            }
        }

      IO.uncancelable { poll =>
        poll(loop(0)).flatten
      }
    }

    override def get: IO[State] = readVersionedState.map(_._1)

  }

}
