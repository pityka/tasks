package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global

import java.net.URI

import scala.concurrent.duration._

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.BillingMode
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import software.amazon.awssdk.services.dynamodb.model.TableStatus

import tasks.queue.DynamoDb
import tasks.queue.QueueImpl
import tasks.util.message.Address
import tasks.util.message.LauncherName

class DynamoDbLiveTest extends FunSuite with Matchers {

  private val liveEnabled = sys.env.get("TASKS_DYNAMODB_LIVE").contains("1")

  private val endpoint = sys.env.get("DYNAMODB_ENDPOINT")

  private val region = sys.env.getOrElse("AWS_REGION", "us-east-1")

  private val partitionKey = "id"

  private def clientResource: Resource[IO, DynamoDbAsyncClient] =
    Resource.fromAutoCloseable(IO.blocking {
      val base = DynamoDbAsyncClient.builder().region(Region.of(region))
      val configured = endpoint match {
        case Some(uri) =>
          base
            .endpointOverride(URI.create(uri))
            .credentialsProvider(
              StaticCredentialsProvider
                .create(AwsBasicCredentials.create("local", "local"))
            )
        case None => base
      }
      configured.build()
    })

  private def waitUntilActive(
      client: DynamoDbAsyncClient,
      table: String
  ): IO[Unit] = {
    val describe = IO.fromCompletableFuture(
      IO(
        client.describeTable(
          DescribeTableRequest.builder().tableName(table).build()
        )
      )
    )
    def loop(remaining: Int): IO[Unit] =
      describe.flatMap { response =>
        if (response.table.tableStatus == TableStatus.ACTIVE) IO.unit
        else if (remaining <= 0)
          IO.raiseError(new RuntimeException(s"table $table never became active"))
        else IO.sleep(500.milliseconds) *> loop(remaining - 1)
      }
    loop(120)
  }

  private def tableResource(
      client: DynamoDbAsyncClient
  ): Resource[IO, String] = {
    val table = "tasks-it-" + java.util.UUID.randomUUID().toString.take(8)
    val create = IO.fromCompletableFuture(
      IO(
        client.createTable(
          CreateTableRequest
            .builder()
            .tableName(table)
            .attributeDefinitions(
              AttributeDefinition
                .builder()
                .attributeName(partitionKey)
                .attributeType(ScalarAttributeType.S)
                .build()
            )
            .keySchema(
              KeySchemaElement
                .builder()
                .attributeName(partitionKey)
                .keyType(KeyType.HASH)
                .build()
            )
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .build()
        )
      )
    ) *> waitUntilActive(client, table).as(table)

    val delete = IO
      .fromCompletableFuture(
        IO(
          client.deleteTable(
            DeleteTableRequest.builder().tableName(table).build()
          )
        )
      )
      .attempt
      .void

    Resource.make(create)(_ => delete)
  }

  private def transactionResource(shardCount: Int) =
    clientResource.flatMap { client =>
      tableResource(client).flatMap { table =>
        DynamoDb
          .makeTransaction(
            client = client,
            table = table,
            stateKey = "tasks-queue-state",
            partitionKeyAttribute = partitionKey,
            retryBaseDelay = 20.milliseconds,
            shardCount = shardCount
          )
      }
    }

  test("a >400KB state commits across shards and reads back") {
    assume(liveEnabled)

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

    val readBack = transactionResource(16)
      .use { tx =>
        tx.flatModify(state =>
          (entries.foldLeft(state)((s, e) => s.update(e)), IO.unit)
        ) *> tx.get
      }
      .unsafeRunSync()

    readBack.completedResults.keySet shouldBe entries.map(_.proxy).toSet
  }

  test("concurrent writers all commit with no lost updates") {
    assume(liveEnabled)

    val writers = 4
    val perWriter = 5

    def addLaunchers(
        tx: tasks.util.Transaction[QueueImpl.State],
        names: List[String]
    ): IO[Unit] =
      names.foldLeft(IO.unit) { (acc, name) =>
        acc *> tx.flatModify(state =>
          (
            state.update(
              QueueImpl.LauncherJoined(LauncherName(name), None)
            ),
            IO.unit
          )
        )
      }

    val expected = (for {
      w <- 0 until writers
      i <- 0 until perWriter
    } yield LauncherName(s"launcher-$w-$i")).toSet

    val finalState = transactionResource(16)
      .use { tx =>
        IO.parSequenceN(writers)(
          (0 until writers).toList.map { w =>
            addLaunchers(
              tx,
              (0 until perWriter).toList.map(i => s"launcher-$w-$i")
            )
          }
        ) *> tx.get
      }
      .unsafeRunSync()

    finalState.knownLaunchers.keySet shouldBe expected
  }

}
