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
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import software.amazon.awssdk.services.s3.model.S3Exception

final class FakeS3AsyncClient(
    val stored: AtomicReference[Option[(String, Array[Byte])]],
    val beforePut: AtomicReference[() => Unit] =
      new AtomicReference(() => ())
) extends S3AsyncClient {

  val putCount = new AtomicInteger(0)
  private val etagCounter = new AtomicInteger(0)

  def serviceName(): String = "s3"

  def close(): Unit = ()

  override def getObject[ReturnT](
      request: GetObjectRequest,
      transformer: AsyncResponseTransformer[GetObjectResponse, ReturnT]
  ): CompletableFuture[ReturnT] = {
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
          .message(
            "At least one of the pre-conditions you specified did not hold"
          )
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
