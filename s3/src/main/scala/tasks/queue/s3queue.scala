package tasks.queue

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource

import scala.concurrent.duration._

import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception

import tasks.queue.QueueImpl.State

object S3QueueState {


  val DefaultRetryBaseDelay = 20.milliseconds

  val preconditionFailedStatusCode = 412

  val conflictingOperationStatusCode = 409

  val DefaultMaxRequestsPerSecond = 100

  val DefaultMaxBurst = 200

  val backpressureWarnInterval = 10.seconds

  def clientResource(
      regionProfileName: Option[String]
  ): Resource[IO, S3AsyncClient] =
    Resource.fromAutoCloseable(IO.blocking {
      tasks.fileservice.s3.S3.makeAWSSDKClient(
        regionProfileName = regionProfileName,
        httpMaxConcurrency = None,
        httpMaxPendingConnectionAcquires = None,
        httpConnectionAcquisitionTimeout = None,
        numRetries = Some(10)
      )
    })

  def makeTransaction(
      bucket: String,
      regionProfileName: Option[String],
      key: String,
      retryBaseDelay: FiniteDuration
  ): Resource[IO, tasks.util.Transaction[State]] =
    makeTransaction(
      bucket = bucket,
      regionProfileName = regionProfileName,
      key = key,
      retryBaseDelay = retryBaseDelay,
      maxRequestsPerSecond = DefaultMaxRequestsPerSecond,
      maxBurst = DefaultMaxBurst
    )

  def makeTransaction(
      bucket: String,
      regionProfileName: Option[String],
      key: String,
      retryBaseDelay: FiniteDuration,
      maxRequestsPerSecond: Int,
      maxBurst: Int
  ): Resource[IO, tasks.util.Transaction[State]] =
    clientResource(regionProfileName).flatMap(client =>
      makeTransaction(
        client = client,
        bucket = bucket,
        key = key,
        retryBaseDelay = retryBaseDelay,
        maxRequestsPerSecond = maxRequestsPerSecond,
        maxBurst = maxBurst
      )
    )

  def makeTransaction(
      client: S3AsyncClient,
      bucket: String,
      key: String,
      retryBaseDelay: FiniteDuration
  ): Resource[IO, tasks.util.Transaction[State]] =
    makeTransaction(
      client = client,
      bucket = bucket,
      key = key,
      retryBaseDelay = retryBaseDelay,
      maxRequestsPerSecond = DefaultMaxRequestsPerSecond,
      maxBurst = DefaultMaxBurst
    )

  def makeTransaction(
      client: S3AsyncClient,
      bucket: String,
      key: String,
      retryBaseDelay: FiniteDuration,
      maxRequestsPerSecond: Int,
      maxBurst: Int
  ): Resource[IO, tasks.util.Transaction[State]] =
    rateLimiter(maxRequestsPerSecond, maxBurst).map(limiter =>
      new S3Transaction(
        client = client,
        bucket = bucket,
        key = key,
        retryBaseDelay = retryBaseDelay,
        limiter = limiter
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

  private[tasks] def rateLimiter(
      maxRequestsPerSecond: Int,
      maxBurst: Int
  ): Resource[IO, RateLimiter] =
    Resource.eval(
      for {
        _ <- IO(
          require(
            maxRequestsPerSecond >= 1 && maxBurst >= 1,
            s"maxRequestsPerSecond and maxBurst must both be >= 1, were $maxRequestsPerSecond and $maxBurst"
          )
        )
        now <- IO.monotonic
        state <- Ref.of[IO, (Long, Long)]((maxBurst.toLong, now.toNanos))
        lastWarnNanos <- Ref.of[IO, Long](
          now.toNanos - backpressureWarnInterval.toNanos
        )
      } yield new RateLimiter(
        state = state,
        refillIntervalNanos =
          math.max(1L, 1000000000L / maxRequestsPerSecond.toLong),
        capacity = maxBurst.toLong,
        lastWarnNanos = lastWarnNanos,
        maxRequestsPerSecond = maxRequestsPerSecond,
        maxBurst = maxBurst
      )
    )

  private[tasks] class RateLimiter(
      state: Ref[IO, (Long, Long)],
      refillIntervalNanos: Long,
      capacity: Long,
      lastWarnNanos: Ref[IO, Long],
      maxRequestsPerSecond: Int,
      maxBurst: Int
  ) {

    def apply[A](io: IO[A]): IO[A] = acquire *> io

    private def acquire: IO[Unit] =
      IO.monotonic.flatMap { nowDuration =>
        val now = nowDuration.toNanos
        state
          .modify { case (tokens, last) =>
            val elapsed = now - last
            val added = if (elapsed <= 0L) 0L else elapsed / refillIntervalNanos
            val tentative = tokens + added
            val (refilled, refilledLast) =
              if (tentative >= capacity) (capacity, now)
              else (tentative, last + added * refillIntervalNanos)
            if (refilled >= 1L)
              ((refilled - 1L, refilledLast), IO.unit)
            else {
              val waitNanos =
                math.max(1L, refillIntervalNanos - (now - refilledLast))
              (
                (refilled, refilledLast),
                warnIfDue *> IO.sleep(waitNanos.nanos) *> acquire
              )
            }
          }
          .flatten
      }

    private def warnIfDue: IO[Unit] =
      IO.monotonic.flatMap { now =>
        lastWarnNanos
          .modify { last =>
            if (now.toNanos - last >= backpressureWarnInterval.toNanos)
              (now.toNanos, true)
            else (last, false)
          }
          .flatMap { due =>
            IO.whenA(due)(
              IO(
                scribe.warn(
                  "S3 queue-state backend is rate-limited: requests are waiting for the token bucket to refill. This caps S3 request cost; raise maxRequestsPerSecond if this is expected load.",
                  scribe.data(
                    Map(
                      "max-requests-per-second" -> maxRequestsPerSecond,
                      "max-burst" -> maxBurst
                    )
                  )
                )
              )
            )
          }
      }
  }

  private[tasks] class S3Transaction(
      client: S3AsyncClient,
      bucket: String,
      key: String,
      retryBaseDelay: FiniteDuration,
      limiter: RateLimiter
  ) extends tasks.util.Transaction[State] {

    private def readVersionedState: IO[(State, Option[String])] = {
      val request = GetObjectRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .build()

      limiter(
        IO.fromCompletableFuture(
          IO(
            client.getObject(
              request,
              AsyncResponseTransformer.toBytes[GetObjectResponse]
            )
          )
        )
      ).map { response =>
        val state =
          SerializableQueueState.decode(decompress(response.asByteArray()))
        (state, Option(response.response().eTag()))
      }.recover { case _: NoSuchKeyException =>
        (State.empty, None)
      }
    }

    private def writeIfUnchanged(
        state: State,
        expectedETag: Option[String]
    ): IO[Boolean] =
      IO(compress(SerializableQueueState.encode(state))).flatMap { payload =>
        val condition = expectedETag match {
          case Some(etag) => AwsRequestOverrideConfiguration
              .builder()
              .putHeader("If-Match", etag)
              .build()
          case None => AwsRequestOverrideConfiguration
              .builder()
              .putHeader("If-None-Match", "*")
              .build()
        }
        val request = PutObjectRequest
          .builder()
          .bucket(bucket)
          .key(key)
          .overrideConfiguration(condition)
          .build()

        limiter(
          IO.fromCompletableFuture(
            IO(client.putObject(request, AsyncRequestBody.fromBytes(payload)))
          )
        ).as(true)
          .recover {
            case e: S3Exception
                if e.statusCode() == preconditionFailedStatusCode ||
                  e.statusCode() == conflictingOperationStatusCode =>
              false
          }
      }

    private def backoff(attempt: Int): IO[Unit] =
      IO.sleep(
        retryBaseDelay * math.pow(2d, math.min(attempt, 5).toDouble).toLong
      )

    override def flatModify[B](update: State => (State, IO[B])): IO[B] = {
      def loop(attempt: Int): IO[IO[B]] =
        readVersionedState.flatMap { case (state, etag) =>
          val (updated, sideEffect) = update(state)
          if (updated == state)
            IO(
              scribe.trace(
                "Queue state unchanged by this update, skipping the write."
              )
            ).as(sideEffect)
          else
            writeIfUnchanged(updated, etag).flatMap { committed =>
              if (committed) IO.pure(sideEffect)
              else
                IO(
                  scribe.debug(
                    "Conditional write of the queue state failed because another process committed first. Try again.",
                    scribe.data(Map("attempt" -> attempt))
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
