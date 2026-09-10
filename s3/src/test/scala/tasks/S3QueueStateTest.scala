package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription

import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import software.amazon.awssdk.services.s3.model.S3Exception

import tasks.queue.QueueImpl
import tasks.queue.S3QueueState
import tasks.queue.SerializableQueueState
import tasks.util.message.LauncherName

class S3QueueStateTest extends FunSuite with Matchers {

  private val bucket = "queue-bucket"
  private val stateKey = "tasks-queue-state"

  private class FakeS3(
      val stored: AtomicReference[Option[(String, Array[Byte])]],
      val beforePut: AtomicReference[() => Unit] = new AtomicReference(() => ())
  ) extends S3AsyncClient {

    val putCount = new AtomicInteger(0)
    private val etagCounter = new AtomicInteger(0)

    def serviceName(): String = "s3"

    def close(): Unit = ()

    override def getObject[ReturnT](
        request: GetObjectRequest,
        transformer: AsyncResponseTransformer[GetObjectResponse, ReturnT]
    ): CompletableFuture[ReturnT] = {
      request.key shouldBe stateKey
      stored.get match {
        case None =>
          CompletableFuture.failedFuture(
            NoSuchKeyException.builder().message("no such key").build()
          )
        case Some((etag, bytes)) =>
          val response = GetObjectResponse.builder().eTag(etag).build()
          CompletableFuture.completedFuture(
            ResponseBytes.fromByteArray(response, bytes).asInstanceOf[ReturnT]
          )
      }
    }

    override def putObject(
        request: PutObjectRequest,
        body: AsyncRequestBody
    ): CompletableFuture[PutObjectResponse] = {
      beforePut.get.apply()
      putCount.incrementAndGet()
      val bytes = drain(body)
      val headers = Option(request.overrideConfiguration().orElse(null))
        .map(_.headers.asScala.map { case (k, v) => (k, v.asScala.toList) }.toMap)
        .getOrElse(Map.empty[String, List[String]])
      val ifMatch = headers.get("If-Match").flatMap(_.headOption)
      val ifNoneMatch = headers.get("If-None-Match").flatMap(_.headOption)
      val current = stored.get
      val passes =
        if (ifNoneMatch.contains("*")) current.isEmpty
        else
          ifMatch match {
            case Some(tag) => current.exists(_._1 == tag)
            case None      => true
          }
      if (!passes)
        CompletableFuture.failedFuture(
          S3Exception
            .builder()
            .statusCode(412)
            .message("At least one of the pre-conditions you specified did not hold")
            .build()
        )
      else {
        val etag = "\"" + etagCounter.incrementAndGet().toString + "\""
        stored.set(Some((etag, bytes)))
        CompletableFuture.completedFuture(
          PutObjectResponse.builder().eTag(etag).build()
        )
      }
    }

    private def drain(body: AsyncRequestBody): Array[Byte] = {
      val out = new java.io.ByteArrayOutputStream()
      val done = new CompletableFuture[Array[Byte]]()
      body.subscribe(new Subscriber[ByteBuffer] {
        def onSubscribe(subscription: Subscription): Unit =
          subscription.request(Long.MaxValue)
        def onNext(buffer: ByteBuffer): Unit = {
          val array = new Array[Byte](buffer.remaining())
          buffer.get(array)
          out.write(array)
        }
        def onError(error: Throwable): Unit = done.completeExceptionally(error)
        def onComplete(): Unit = done.complete(out.toByteArray)
      })
      done.get()
    }
  }

  private def transactionFor(client: S3AsyncClient) =
    S3QueueState.makeTransaction(
      client = client,
      bucket = bucket,
      key = stateKey,
      retryBaseDelay = 1.millisecond
    )

  private def storedState(client: FakeS3): QueueImpl.State =
    SerializableQueueState.decode(
      S3QueueState.decompress(client.stored.get.get._2)
    )

  test("an absent object reads as the empty state") {
    val client = new FakeS3(new AtomicReference(None))
    transactionFor(client)
      .use(_.get)
      .unsafeRunSync() shouldBe QueueImpl.State.empty
  }

  test("flatModify commits the new state and creates the object") {
    val client = new FakeS3(new AtomicReference(None))

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
    client.stored.get.isDefined shouldBe true
    storedState(client).knownLaunchers.keySet shouldBe Set(
      LauncherName("launcher-1")
    )
  }

  test("the side effect runs only after the conditional write commits") {
    val client = new FakeS3(new AtomicReference(None))
    val sideEffectRan = new AtomicInteger(0)
    val putsAtSideEffect = new AtomicInteger(-1)

    transactionFor(client)
      .use(tx =>
        tx.flatModify(state =>
          (
            state.update(
              QueueImpl.LauncherJoined(LauncherName("launcher-1"), None)
            ),
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

  test("an update that leaves the state unchanged does not write") {
    val client = new FakeS3(new AtomicReference(None))
    val sideEffectRan = new AtomicInteger(0)

    val result = transactionFor(client)
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
    client.putCount.get shouldBe 0
    client.stored.get shouldBe None
  }

  test("a competing commit forces a retry and the update is reapplied") {
    val client = new FakeS3(new AtomicReference(None))
    val updateInvocations = new AtomicInteger(0)

    client.beforePut.set { () =>
      if (updateInvocations.get == 1)
        client.stored.set(
          Some(
            (
              "\"competitor-etag\"",
              S3QueueState.compress(
                SerializableQueueState.encode(
                  QueueImpl.State.empty.update(
                    QueueImpl.LauncherJoined(LauncherName("competitor"), None)
                  )
                )
              )
            )
          )
        )
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
    storedState(client).knownLaunchers.keySet shouldBe Set(
      LauncherName("competitor"),
      LauncherName("mine")
    )
  }

  test("compress and decompress round trip an encoded state") {
    val encoded = SerializableQueueState.encode(
      QueueImpl.State.empty.update(
        QueueImpl.Incremented(LauncherName("launcher-1"))
      )
    )
    S3QueueState.decompress(S3QueueState.compress(encoded)).toList shouldBe
      encoded.toList
  }

}
