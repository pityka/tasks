package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global

import java.net.URI

import scala.concurrent.duration._

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest

import tasks.queue.QueueImpl
import tasks.queue.S3QueueState
import tasks.util.message.Address
import tasks.util.message.LauncherName

class S3QueueStateLiveTest extends FunSuite with Matchers {

  private val liveEnabled = sys.env.get("TASKS_S3_LIVE").contains("1")

  private val bucketOption = sys.env.get("S3_BUCKET")

  private val endpoint = sys.env.get("S3_ENDPOINT")

  private val region = sys.env.getOrElse("AWS_REGION", "us-east-1")

  private def clientResource: Resource[IO, S3AsyncClient] =
    Resource.fromAutoCloseable(IO.blocking {
      val base = S3AsyncClient
        .builder()
        .region(Region.of(region))
        .credentialsProvider(DefaultCredentialsProvider.create())
      val configured = endpoint match {
        case Some(uri) =>
          base.endpointOverride(URI.create(uri)).forcePathStyle(true)
        case None => base
      }
      configured.build()
    })

  private def keyResource(
      client: S3AsyncClient,
      bucket: String
  ): Resource[IO, String] = {
    val key = "tasks-it/" + java.util.UUID.randomUUID().toString
    Resource.make(IO.pure(key))(k =>
      IO.fromCompletableFuture(
        IO(
          client.deleteObject(
            DeleteObjectRequest.builder().bucket(bucket).key(k).build()
          )
        )
      ).attempt.void
    )
  }

  private def transactionResource(
      bucket: String
  ): Resource[IO, tasks.util.Transaction[QueueImpl.State]] =
    clientResource.flatMap { client =>
      keyResource(client, bucket).flatMap { key =>
        S3QueueState.makeTransaction(
          client = client,
          bucket = bucket,
          key = key,
          retryBaseDelay = 20.milliseconds
        )
      }
    }

  test("a >400KB state commits to a single object and reads back") {
    assume(liveEnabled)
    val bucket = bucketOption.getOrElse(cancel("S3_BUCKET must be set"))

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

    val readBack = transactionResource(bucket)
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
    val bucket = bucketOption.getOrElse(cancel("S3_BUCKET must be set"))

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

    val finalState = transactionResource(bucket)
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
