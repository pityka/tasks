package tasks

import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import scala.jdk.CollectionConverters._

import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription

import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import software.amazon.awssdk.services.s3.model.S3Exception

final class FakeS3AsyncClient(
    initial: Map[String, (String, Array[Byte])] =
      Map.empty[String, (String, Array[Byte])],
    val beforePut: AtomicReference[() => Unit] = new AtomicReference(() => ())
) extends S3AsyncClient {

  private val keyStore: AtomicReference[Map[String, (String, Array[Byte])]] =
    new AtomicReference(initial)

  val putCount = new AtomicInteger(0)
  val deleteCount = new AtomicInteger(0)
  val getCount = new AtomicInteger(0)

  private val etagCounter = new AtomicInteger(0)

  def storedFor(key: String): Option[(String, Array[Byte])] =
    keyStore.get.get(key)

  def setStored(key: String, value: (String, Array[Byte])): Unit =
    keyStore.updateAndGet(_ + (key -> value))

  def removeStored(key: String): Unit =
    keyStore.updateAndGet(_ - key)

  def serviceName(): String = "s3"

  def close(): Unit = ()

  override def getObject[ReturnT](
      request: GetObjectRequest,
      transformer: AsyncResponseTransformer[GetObjectResponse, ReturnT]
  ): CompletableFuture[ReturnT] = {
    getCount.incrementAndGet()
    val key = request.key()
    val headers = Option(request.overrideConfiguration().orElse(null))
      .map(_.headers.asScala.map { case (k, v) => (k, v.asScala.toList) }.toMap)
      .getOrElse(Map.empty[String, List[String]])
    val ifNoneMatch =
      Option(request.ifNoneMatch())
        .orElse(headers.get("If-None-Match").flatMap(_.headOption))
    keyStore.get.get(key) match {
      case None =>
        CompletableFuture.failedFuture(
          NoSuchKeyException.builder().message("no such key").build()
        )
      case Some((etag, bytes)) if ifNoneMatch.contains(etag) =>
        CompletableFuture.failedFuture(
          S3Exception
            .builder()
            .statusCode(304)
            .message("Not Modified")
            .build()
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
  ): CompletableFuture[PutObjectResponse] = this.synchronized {
    beforePut.get.apply()
    putCount.incrementAndGet()
    val key = request.key()
    val bytes = drain(body)
    val headers = Option(request.overrideConfiguration().orElse(null))
      .map(_.headers.asScala.map { case (k, v) => (k, v.asScala.toList) }.toMap)
      .getOrElse(Map.empty[String, List[String]])
    val ifMatch = headers.get("If-Match").flatMap(_.headOption)
    val ifNoneMatch = headers.get("If-None-Match").flatMap(_.headOption)
    val current = keyStore.get.get(key)
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
          .message(
            "At least one of the pre-conditions you specified did not hold"
          )
          .build()
      )
    else {
      val etag = "\"" + etagCounter.incrementAndGet().toString + "\""
      keyStore.updateAndGet(_ + (key -> ((etag, bytes))))
      CompletableFuture.completedFuture(
        PutObjectResponse.builder().eTag(etag).build()
      )
    }
  }

  override def deleteObject(
      request: DeleteObjectRequest
  ): CompletableFuture[DeleteObjectResponse] = this.synchronized {
    deleteCount.incrementAndGet()
    val key = request.key()
    val headers = Option(request.overrideConfiguration().orElse(null))
      .map(_.headers.asScala.map { case (k, v) => (k, v.asScala.toList) }.toMap)
      .getOrElse(Map.empty[String, List[String]])
    val ifMatch = headers.get("If-Match").flatMap(_.headOption)
    val current = keyStore.get.get(key)
    val passes = ifMatch match {
      case Some(tag) => current.exists(_._1 == tag)
      case None      => true
    }
    if (!passes)
      CompletableFuture.failedFuture(
        S3Exception
          .builder()
          .statusCode(412)
          .message("precondition failed")
          .build()
      )
    else {
      keyStore.updateAndGet(_ - key)
      CompletableFuture.completedFuture(
        DeleteObjectResponse.builder().build()
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
