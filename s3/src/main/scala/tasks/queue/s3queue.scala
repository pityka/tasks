package tasks.queue

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import cats.effect.std.Mutex

import scala.concurrent.duration._

import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros._

import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception

import tasks.queue.QueueImpl.State

object S3QueueState {

  val DefaultRetryBaseDelay = 20.milliseconds

  val DefaultYieldDelay = 150.milliseconds

  case class LeaseConfig(
      key: String,
      ttl: FiniteDuration,
      acquireBaseDelay: FiniteDuration
  )

  object LeaseConfig {
    val DefaultTtl: FiniteDuration = 30.seconds
    val DefaultAcquireBaseDelay: FiniteDuration = 50.milliseconds

    def forStateKey(stateKey: String): LeaseConfig =
      LeaseConfig(
        key = stateKey + ".lease",
        ttl = DefaultTtl,
        acquireBaseDelay = DefaultAcquireBaseDelay
      )
  }

  private[tasks] case class LeaseBody(h: String, e: Long)
  private[tasks] object LeaseBody {
    implicit val codec: JsonValueCodec[LeaseBody] = JsonCodecMaker.make
  }

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
    makeTransaction(
      bucket = bucket,
      regionProfileName = regionProfileName,
      key = key,
      retryBaseDelay = retryBaseDelay,
      rateLimits = rateLimits,
      yieldDelay = DefaultYieldDelay
    )

  def makeTransaction(
      bucket: String,
      regionProfileName: Option[String],
      key: String,
      retryBaseDelay: FiniteDuration,
      rateLimits: RateLimits,
      yieldDelay: FiniteDuration
  ): Resource[IO, tasks.util.Transaction[State]] =
    clientResource(regionProfileName).flatMap(client =>
      makeTransaction(
        client = client,
        bucket = bucket,
        key = key,
        retryBaseDelay = retryBaseDelay,
        rateLimits = rateLimits,
        yieldDelay = yieldDelay
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
      yieldDelay: FiniteDuration
  ): Resource[IO, tasks.util.Transaction[State]] =
    makeTransaction(
      client = client,
      bucket = bucket,
      key = key,
      retryBaseDelay = retryBaseDelay,
      rateLimits = RateLimits.default,
      yieldDelay = yieldDelay
    )

  def makeTransaction(
      client: S3AsyncClient,
      bucket: String,
      key: String,
      retryBaseDelay: FiniteDuration,
      rateLimits: RateLimits
  ): Resource[IO, tasks.util.Transaction[State]] =
    makeTransaction(
      client = client,
      bucket = bucket,
      key = key,
      retryBaseDelay = retryBaseDelay,
      rateLimits = rateLimits,
      yieldDelay = DefaultYieldDelay
    )

  def makeTransaction(
      client: S3AsyncClient,
      bucket: String,
      key: String,
      retryBaseDelay: FiniteDuration,
      rateLimits: RateLimits,
      yieldDelay: FiniteDuration
  ): Resource[IO, tasks.util.Transaction[State]] =
    makeTransaction(
      client = client,
      bucket = bucket,
      key = key,
      retryBaseDelay = retryBaseDelay,
      rateLimits = rateLimits,
      yieldDelay = yieldDelay,
      leaseConfig = Some(LeaseConfig.forStateKey(key))
    )

  def makeTransaction(
      client: S3AsyncClient,
      bucket: String,
      key: String,
      retryBaseDelay: FiniteDuration,
      leaseConfig: Option[LeaseConfig]
  ): Resource[IO, tasks.util.Transaction[State]] =
    makeTransaction(
      client = client,
      bucket = bucket,
      key = key,
      retryBaseDelay = retryBaseDelay,
      rateLimits = RateLimits.default,
      yieldDelay = DefaultYieldDelay,
      leaseConfig = leaseConfig
    )

  def makeTransaction(
      client: S3AsyncClient,
      bucket: String,
      key: String,
      retryBaseDelay: FiniteDuration,
      rateLimits: RateLimits,
      yieldDelay: FiniteDuration,
      leaseConfig: Option[LeaseConfig]
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
      yieldUntilNanos <- Resource.eval(Ref.of[IO, Long](0L))
      leaseHeld <- Resource.eval(Ref.of[IO, Option[String]](None))
      holderId <- Resource.eval(IO(java.util.UUID.randomUUID().toString))
      writeMutex <- Resource.eval(Mutex[IO])
      readMutex <- Resource.eval(Mutex[IO])
    } yield new S3Transaction(
      client = client,
      bucket = bucket,
      key = key,
      retryBaseDelay = retryBaseDelay,
      yieldDelay = yieldDelay,
      leaseConfig = leaseConfig,
      holderId = holderId,
      readLimiter = readLimiter,
      writeLimiter = writeLimiter,
      cached = cached,
      yieldUntilNanos = yieldUntilNanos,
      leaseHeld = leaseHeld,
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
      yieldDelay: FiniteDuration,
      leaseConfig: Option[LeaseConfig],
      holderId: String,
      readLimiter: RateLimiter,
      writeLimiter: RateLimiter,
      cached: Ref[IO, Option[Cached]],
      yieldUntilNanos: Ref[IO, Long],
      leaseHeld: Ref[IO, Option[String]],
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
        val previousEtag = current.flatMap(_.etag)
        conditionalGet(previousEtag).flatMap { result =>
          IO.monotonic.flatMap { now =>
            val (state, etag) = result match {
              case ReadNotModified =>
                (
                  current.map(_.state).getOrElse(State.empty),
                  previousEtag
                )
              case ReadLoaded(s, e) => (s, Some(e))
              case ReadAbsent       => (State.empty, None)
            }
            val sawCompetitor = result match {
              case ReadLoaded(_, e) if !previousEtag.contains(e) => true
              case _                                             => false
            }
            val noteCompetitor =
              if (sawCompetitor) armYield(now.toNanos) else IO.unit
            cached.set(Some(Cached(state, etag, now.toNanos))) *>
              noteCompetitor.as((state, etag))
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

    private def armYield(nowNanos: Long): IO[Unit] =
      yieldUntilNanos.update(prev =>
        math.max(prev, nowNanos + yieldDelay.toNanos)
      )

    private def yieldIfPending: IO[Unit] =
      IO.monotonic.flatMap { now =>
        yieldUntilNanos.get.flatMap { until =>
          val deltaNanos = until - now.toNanos
          if (deltaNanos > 0)
            IO.sleep(
              FiniteDuration(
                deltaNanos,
                java.util.concurrent.TimeUnit.NANOSECONDS
              )
            )
          else IO.unit
        }
      }

    private def jitteredRetry: IO[Unit] = {
      val baseNanos = retryBaseDelay.toNanos
      val jitterNanos = (scala.util.Random.nextDouble() * baseNanos).toLong
      IO.sleep(
        FiniteDuration(
          baseNanos + jitterNanos,
          java.util.concurrent.TimeUnit.NANOSECONDS
        )
      )
    }

    override def get: IO[State] = readLatest.map(_._1)

    private def readLeaseIfExists: IO[Option[(LeaseBody, String)]] = {
      val cfg = leaseConfig.get
      val request =
        GetObjectRequest.builder().bucket(bucket).key(cfg.key).build()
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
        val body = readFromArray[LeaseBody](response.asByteArray())
        Some((body, response.response().eTag())): Option[(LeaseBody, String)]
      }.recover { case _: NoSuchKeyException => None }
    }

    private def putLease(
        bytes: Array[Byte],
        condition: AwsRequestOverrideConfiguration
    ): IO[Option[String]] = {
      val cfg = leaseConfig.get
      val request = PutObjectRequest
        .builder()
        .bucket(bucket)
        .key(cfg.key)
        .overrideConfiguration(condition)
        .build()
      writeLimiter(
        IO.fromCompletableFuture(
          IO(client.putObject(request, AsyncRequestBody.fromBytes(bytes)))
        )
      ).map(response => Option(response.eTag())).recover {
        case e: S3Exception
            if e.statusCode() == preconditionFailedStatusCode ||
              e.statusCode() == conflictingOperationStatusCode =>
          None
      }
    }

    private def acquireLease: IO[String] = {
      val cfg = leaseConfig.get
      def tryOnce: IO[Option[String]] =
        IO.monotonic.flatMap { now =>
          val expiresAtNanos = now.toNanos + cfg.ttl.toNanos
          val bytes = writeToArray(LeaseBody(holderId, expiresAtNanos))
          readLeaseIfExists.flatMap {
            case None =>
              val cond = AwsRequestOverrideConfiguration
                .builder()
                .putHeader("If-None-Match", "*")
                .build()
              putLease(bytes, cond)
            case Some((body, existingETag)) =>
              val mine = body.h == holderId
              val expired = body.e <= now.toNanos
              if (!mine && !expired) IO.pure(None)
              else {
                val cond = AwsRequestOverrideConfiguration
                  .builder()
                  .putHeader("If-Match", existingETag)
                  .build()
                putLease(bytes, cond)
              }
          }
        }

      def loop: IO[String] =
        tryOnce.flatMap {
          case Some(etag) => leaseHeld.set(Some(etag)).as(etag)
          case None =>
            jitteredLeaseRetry *>
              IO.monotonic.flatMap(now => armYield(now.toNanos)) *>
              loop
        }

      loop
    }

    private def releaseLease: IO[Unit] =
      leaseConfig match {
        case None => IO.unit
        case Some(cfg) =>
          leaseHeld.getAndSet(None).flatMap {
            case None => IO.unit
            case Some(etag) =>
              val cond = AwsRequestOverrideConfiguration
                .builder()
                .putHeader("If-Match", etag)
                .build()
              val request = DeleteObjectRequest
                .builder()
                .bucket(bucket)
                .key(cfg.key)
                .overrideConfiguration(cond)
                .build()
              writeLimiter(
                IO.fromCompletableFuture(IO(client.deleteObject(request)))
              ).void.recover { case _: S3Exception => () }
          }
      }

    private def jitteredLeaseRetry: IO[Unit] = {
      val cfg = leaseConfig.get
      val baseNanos = cfg.acquireBaseDelay.toNanos
      val jitterNanos = (scala.util.Random.nextDouble() * baseNanos).toLong
      IO.sleep(
        FiniteDuration(
          baseNanos + jitterNanos,
          java.util.concurrent.TimeUnit.NANOSECONDS
        )
      )
    }

    override def flatModify[B](update: State => (State, IO[B])): IO[B] =
      leaseConfig match {
        case None    => flatModifyCas(update)
        case Some(_) => flatModifyLeased(update)
      }

    private def flatModifyLeased[B](update: State => (State, IO[B])): IO[B] = {
      def attempt(attemptNo: Int): IO[IO[B]] = {
        val preRead: IO[Unit] =
          if (attemptNo == 0) readLatest.void else IO.unit
        preRead *> yieldIfPending *>
          acquireLease
            .bracket { _ =>
              forceRead.flatMap { case (state, stateETag) =>
                val (updated, sideEffect) = update(state)
                if (updated == state)
                  IO.pure(Right(sideEffect): Either[Unit, IO[B]])
                else
                  putIfMatch(updated, stateETag).flatMap {
                    case Some(newETag) =>
                      IO.monotonic.flatMap { now =>
                        val noteFight =
                          if (attemptNo > 0) armYield(now.toNanos) else IO.unit
                        cached.set(
                          Some(Cached(updated, Some(newETag), now.toNanos))
                        ) *> noteFight.as(
                          Right(sideEffect): Either[Unit, IO[B]]
                        )
                      }
                    case None =>
                      IO(
                        scribe.warn(
                          "S3 lease fencing: our state PUT under a held lease was rejected (likely a stolen lease). Retrying."
                        )
                      ) *>
                        IO.monotonic
                          .flatMap(now => armYield(now.toNanos))
                          .as(Left(()): Either[Unit, IO[B]])
                  }
              }
            }(_ => releaseLease)
            .flatMap {
              case Right(se) => IO.pure(se)
              case Left(_)   => jitteredRetry *> attempt(attemptNo + 1)
            }
      }

      IO.uncancelable(poll => poll(attempt(0)).flatten)
    }

    private def flatModifyCas[B](update: State => (State, IO[B])): IO[B] = {
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
                IO.monotonic.flatMap { now =>
                  val noteFight =
                    if (attempt > 0) armYield(now.toNanos) else IO.unit
                  cached.set(
                    Some(Cached(updated, Some(newETag), now.toNanos))
                  ) *>
                    noteFight.as(sideEffect)
                }
              case None =>
                IO {
                  val message =
                    "Conditional write of the queue state failed because another process committed first. Try again."
                  val data = scribe.data(Map("attempt" -> attempt))
                  if (attempt > 10) scribe.warn(message, data)
                  else scribe.debug(message, data)
                } *> forceRead *> jitteredRetry *> commit(attempt + 1, None)
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
            yieldIfPending *>
              writeMutex.lock.surround(
                commit(0, Some((readETag, updated, sideEffect)))
              )
        }

      IO.uncancelable(poll => poll(start).flatten)
    }

  }

}
