package tasks.queue

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import cats.effect.std.Mutex

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

  val notModifiedStatusCode = 304

  val DefaultMaxReadRequestsPerSecond = 300

  val DefaultMaxReadBurst = 400

  val DefaultMaxWriteRequestsPerSecond = 100

  val DefaultMaxWriteBurst = 200

  val DefaultReadCacheTtl = 1.second

  val backpressureWarnInterval = 10.seconds

  case class RateLimits(
      maxReadRequestsPerSecond: Int,
      maxReadBurst: Int,
      maxWriteRequestsPerSecond: Int,
      maxWriteBurst: Int
  )

  object RateLimits {
    val default: RateLimits = RateLimits(
      maxReadRequestsPerSecond = DefaultMaxReadRequestsPerSecond,
      maxReadBurst = DefaultMaxReadBurst,
      maxWriteRequestsPerSecond = DefaultMaxWriteRequestsPerSecond,
      maxWriteBurst = DefaultMaxWriteBurst
    )
  }

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
      rateLimits = RateLimits.default
    )

  def makeTransaction(
      bucket: String,
      regionProfileName: Option[String],
      key: String,
      retryBaseDelay: FiniteDuration,
      rateLimits: RateLimits
  ): Resource[IO, tasks.util.Transaction[State]] =
    clientResource(regionProfileName).flatMap(client =>
      makeTransaction(
        client = client,
        bucket = bucket,
        key = key,
        retryBaseDelay = retryBaseDelay,
        rateLimits = rateLimits
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
      rateLimits = RateLimits.default
    )

  def makeTransaction(
      client: S3AsyncClient,
      bucket: String,
      key: String,
      retryBaseDelay: FiniteDuration,
      rateLimits: RateLimits
  ): Resource[IO, tasks.util.Transaction[State]] =
    for {
      readLimiter <- rateLimiter(
        "read",
        rateLimits.maxReadRequestsPerSecond,
        rateLimits.maxReadBurst
      )
      writeLimiter <- rateLimiter(
        "write",
        rateLimits.maxWriteRequestsPerSecond,
        rateLimits.maxWriteBurst
      )
      cached <- Resource.eval(Ref.of[IO, Option[Cached]](None))
      writeMutex <- Resource.eval(Mutex[IO])
      readMutex <- Resource.eval(Mutex[IO])
    } yield new S3Transaction(
      client = client,
      bucket = bucket,
      key = key,
      retryBaseDelay = retryBaseDelay,
      readLimiter = readLimiter,
      writeLimiter = writeLimiter,
      cached = cached,
      writeMutex = writeMutex,
      readMutex = readMutex,
      readCacheTtl = DefaultReadCacheTtl
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
      label: String,
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
        label = label,
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
      label: String,
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
        state.modify { case (tokens, last) =>
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
        }.flatten
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
                  s"S3 queue-state backend $label requests are rate-limited: waiting for the token bucket to refill. This caps S3 request cost; raise the corresponding limit if this is expected load.",
                  scribe.data(
                    Map(
                      "limiter" -> label,
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

  private sealed trait ReadResult
  private case object ReadNotModified extends ReadResult
  private case class ReadLoaded(state: State, etag: String) extends ReadResult
  private case object ReadAbsent extends ReadResult

  private[tasks] case class Cached(
      state: State,
      etag: Option[String],
      confirmedAtNanos: Long
  )

  private[tasks] class S3Transaction(
      client: S3AsyncClient,
      bucket: String,
      key: String,
      retryBaseDelay: FiniteDuration,
      readLimiter: RateLimiter,
      writeLimiter: RateLimiter,
      cached: Ref[IO, Option[Cached]],
      writeMutex: Mutex[IO],
      readMutex: Mutex[IO],
      readCacheTtl: FiniteDuration
  ) extends tasks.util.Transaction[State] {

    private def conditionalGet(ifNoneMatch: Option[String]): IO[ReadResult] = {
      val base = GetObjectRequest.builder().bucket(bucket).key(key)
      val request =
        ifNoneMatch.fold(base)(etag => base.ifNoneMatch(etag)).build()
      readLimiter(
        IO.fromCompletableFuture(
          IO(
            client.getObject(
              request,
              AsyncResponseTransformer.toBytes[GetObjectResponse]
            )
          )
        )
      ).map { response =>
        (ReadLoaded(
          SerializableQueueState.decode(decompress(response.asByteArray())),
          response.response().eTag()
        ): ReadResult)
      }.recover {
        case _: NoSuchKeyException => ReadAbsent
        case e: S3Exception if e.statusCode() == notModifiedStatusCode =>
          ReadNotModified
      }
    }

    private def forceRead: IO[(State, Option[String])] =
      cached.get.flatMap { current =>
        conditionalGet(current.flatMap(_.etag)).flatMap { result =>
          IO.monotonic.flatMap { now =>
            val (state, etag) = result match {
              case ReadNotModified =>
                (
                  current.map(_.state).getOrElse(State.empty),
                  current.flatMap(_.etag)
                )
              case ReadLoaded(s, e) => (s, Some(e))
              case ReadAbsent       => (State.empty, None)
            }
            cached.set(Some(Cached(state, etag, now.toNanos))).as((state, etag))
          }
        }
      }

    private def readLatest: IO[(State, Option[String])] =
      IO.monotonic.flatMap { now =>
        cached.get.flatMap {
          case Some(c)
              if now.toNanos - c.confirmedAtNanos <= readCacheTtl.toNanos =>
            IO.pure((c.state, c.etag))
          case _ =>
            readMutex.lock.surround {
              IO.monotonic.flatMap { now2 =>
                cached.get.flatMap {
                  case Some(c)
                      if now2.toNanos - c.confirmedAtNanos <= readCacheTtl.toNanos =>
                    IO.pure((c.state, c.etag))
                  case _ => forceRead
                }
              }
            }
        }
      }

    private def putIfMatch(
        state: State,
        expectedETag: Option[String]
    ): IO[Option[String]] =
      IO(compress(SerializableQueueState.encode(state))).flatMap { payload =>
        val condition = expectedETag match {
          case Some(etag) =>
            AwsRequestOverrideConfiguration
              .builder()
              .putHeader("If-Match", etag)
              .build()
          case None =>
            AwsRequestOverrideConfiguration
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

        writeLimiter(
          IO.fromCompletableFuture(
            IO(client.putObject(request, AsyncRequestBody.fromBytes(payload)))
          )
        ).map(response => Option(response.eTag()))
          .recover {
            case e: S3Exception
                if e.statusCode() == preconditionFailedStatusCode ||
                  e.statusCode() == conflictingOperationStatusCode =>
              None
          }
      }

    private def backoff(attempt: Int): IO[Unit] =
      IO.sleep(
        retryBaseDelay * math.pow(2d, math.min(attempt, 5).toDouble).toLong
      )

    override def get: IO[State] = readLatest.map(_._1)

    override def flatModify[B](update: State => (State, IO[B])): IO[B] = {
      def commit(
          attempt: Int,
          reusable: Option[(Option[String], State, IO[B])]
      ): IO[IO[B]] =
        cached.get.flatMap { current =>
          val currentState = current.map(_.state).getOrElse(State.empty)
          val currentETag = current.flatMap(_.etag)
          val (base, updated, sideEffect) = reusable match {
            case Some((readETag, computed, effect))
                if readETag == currentETag =>
              (currentState, computed, effect)
            case _ =>
              val (recomputed, effect) = update(currentState)
              (currentState, recomputed, effect)
          }
          if (updated == base) IO.pure(sideEffect)
          else
            putIfMatch(updated, currentETag).flatMap {
              case Some(newETag) =>
                IO.monotonic
                  .flatMap(now =>
                    cached
                      .set(Some(Cached(updated, Some(newETag), now.toNanos)))
                  )
                  .as(sideEffect)
              case None =>
                IO {
                  val message =
                    "Conditional write of the queue state failed because another process committed first. Try again."
                  val data = scribe.data(Map("attempt" -> attempt))
                  if (attempt > 10) scribe.warn(message, data)
                  else scribe.debug(message, data)
                } *> forceRead *> backoff(attempt) *> commit(
                  attempt + 1,
                  None
                )
            }
        }

      def start: IO[IO[B]] =
        readLatest.flatMap { case (state, readETag) =>
          val (updated, sideEffect) = update(state)
          if (updated == state)
            IO(
              scribe.trace(
                "Queue state unchanged by this update, skipping the write."
              )
            ).as(sideEffect)
          else
            writeMutex.lock.surround(
              commit(0, Some((readETag, updated, sideEffect)))
            )
        }

      IO.uncancelable(poll => poll(start).flatten)
    }

  }

}
