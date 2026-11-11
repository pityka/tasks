package tasks.fileservice.s3

import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite
import scodec.bits.ByteVector
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{
  GetObjectRequest,
  GetObjectResponse
}

import java.nio.file.{Files, Path}

class S3ReadTest extends AnyFunSuite {

  private def anonymousClient(region: Region): S3AsyncClient =
    S3AsyncClient
      .builder()
      .credentialsProvider(AnonymousCredentialsProvider.create())
      .region(region)
      .httpClient(
        NettyNioAsyncHttpClient.builder().maxConcurrency(256).build()
      )
      .build()

  // Sentinel-2 L2A - public bucket in eu-central-1, ~105 MB file
  private val bucket = "sentinel-s2-l2a"
  private val key = "tiles/33/U/UP/2024/7/1/0/R10m/B08.jp2"

  private val region = Region.EU_CENTRAL_1

  private case class Config(partSizeMB: Int, concurrency: Int)

  private def benchmark(s3: S3, cfg: Config): (Long, Long) = {
    val t0 = System.nanoTime()
    val bytes =
      s3.readFileMultipart(bucket, key, cfg.partSizeMB, cfg.concurrency)
        .compile
        .count
        .unsafeRunSync()
    val elapsed = (System.nanoTime() - t0) / 1000000
    (bytes, elapsed)
  }

  private def withS3[A](f: S3 => A): A = {
    val client = anonymousClient(region)
    try f(new S3(client, None))
    finally client.close()
  }

  // Read just the tail of the file so all correctness tests stay fast.
  private val tailBytes: Long = 256L * 1024L

  test("readFile fromOffset reads exactly contentLength - fromOffset bytes") {
    withS3 { s3 =>
      val total =
        s3.getObjectMetadata(bucket, key).unsafeRunSync().get.contentLength
      val offset = total - tailBytes
      val count = s3
        .readFile(bucket, key, fromOffset = offset)
        .compile
        .count
        .unsafeRunSync()
      assert(count == tailBytes)
    }
  }

  test("readFileMultipart fromOffset reads exactly contentLength - fromOffset bytes") {
    withS3 { s3 =>
      val total =
        s3.getObjectMetadata(bucket, key).unsafeRunSync().get.contentLength
      val offset = total - tailBytes
      val count = s3
        .readFileMultipart(
          bucket,
          key,
          partSize = 1,
          multiPartConcurrency = 4,
          fromOffset = offset
        )
        .compile
        .count
        .unsafeRunSync()
      assert(count == tailBytes)
    }
  }

  test("readFile and readFileMultipart return identical bytes from the same fromOffset") {
    withS3 { s3 =>
      val total =
        s3.getObjectMetadata(bucket, key).unsafeRunSync().get.contentLength
      val offset = total - tailBytes
      val single = s3
        .readFile(bucket, key, fromOffset = offset)
        .compile
        .to(ByteVector)
        .unsafeRunSync()
      val multi = s3
        .readFileMultipart(
          bucket,
          key,
          partSize = 1,
          multiPartConcurrency = 4,
          fromOffset = offset
        )
        .compile
        .to(ByteVector)
        .unsafeRunSync()
      assert(single.length == tailBytes)
      assert(multi == single)
    }
  }

  test("readFile fromOffset = N equals readFile fromOffset = N - delta dropping delta bytes") {
    withS3 { s3 =>
      val total =
        s3.getObjectMetadata(bucket, key).unsafeRunSync().get.contentLength
      val delta = 1024L
      val offsetHi = total - tailBytes
      val offsetLo = offsetHi - delta
      val direct = s3
        .readFile(bucket, key, fromOffset = offsetHi)
        .compile
        .to(ByteVector)
        .unsafeRunSync()
      val droppedPrefix = s3
        .readFile(bucket, key, fromOffset = offsetLo)
        .drop(delta)
        .compile
        .to(ByteVector)
        .unsafeRunSync()
      assert(direct == droppedPrefix)
    }
  }

  test("readFileMultipart fromOffset = N equals readFileMultipart fromOffset = N - delta dropping delta bytes") {
    withS3 { s3 =>
      val total =
        s3.getObjectMetadata(bucket, key).unsafeRunSync().get.contentLength
      // delta smaller than partSize so the lower-offset read spans two parts.
      val partSize = 1
      val delta = 1024L
      val offsetHi = total - tailBytes
      val offsetLo = offsetHi - delta
      val direct = s3
        .readFileMultipart(
          bucket,
          key,
          partSize = partSize,
          multiPartConcurrency = 4,
          fromOffset = offsetHi
        )
        .compile
        .to(ByteVector)
        .unsafeRunSync()
      val droppedPrefix = s3
        .readFileMultipart(
          bucket,
          key,
          partSize = partSize,
          multiPartConcurrency = 4,
          fromOffset = offsetLo
        )
        .drop(delta)
        .compile
        .to(ByteVector)
        .unsafeRunSync()
      assert(direct == droppedPrefix)
    }
  }

  test("readFileMultipart fromOffset is consistent across partSize / concurrency configs") {
    withS3 { s3 =>
      val total =
        s3.getObjectMetadata(bucket, key).unsafeRunSync().get.contentLength
      val offset = total - tailBytes
      val configs = List((1, 1), (1, 4), (2, 2), (5, 1))
      val results = configs.map { case (partSize, concurrency) =>
        s3.readFileMultipart(
          bucket,
          key,
          partSize = partSize,
          multiPartConcurrency = concurrency,
          fromOffset = offset
        ).compile.to(ByteVector).unsafeRunSync()
      }
      val first = results.head
      assert(first.length == tailBytes)
      results.tail.foreach(r => assert(r == first))
    }
  }

  // Independent oracle: download a range straight to a local file using the
  // AWS SDK's AsyncResponseTransformer.toFile path, which shares no code with
  // our fs2 stream operators.
  private def downloadRangeToFile(
      s3: S3,
      offset: Long
  ): ByteVector = {
    val path: Path = Files.createTempFile("s3-stream-test-", ".bin")
    Files.delete(path) // toFile refuses to overwrite an existing file
    try {
      val req = GetObjectRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .range(s"bytes=$offset-")
        .build()
      s3.s3
        .getObject(req, AsyncResponseTransformer.toFile[GetObjectResponse](path))
        .get()
      ByteVector.view(Files.readAllBytes(path))
    } finally Files.deleteIfExists(path)
  }

  test("readFile fromOffset matches AsyncResponseTransformer.toFile download") {
    withS3 { s3 =>
      val total =
        s3.getObjectMetadata(bucket, key).unsafeRunSync().get.contentLength
      val offset = total - tailBytes
      val oracle = downloadRangeToFile(s3, offset)
      val streamed = s3
        .readFile(bucket, key, fromOffset = offset)
        .compile
        .to(ByteVector)
        .unsafeRunSync()
      assert(oracle.length == tailBytes)
      assert(streamed == oracle)
    }
  }

  test("readFileMultipart fromOffset matches AsyncResponseTransformer.toFile download") {
    withS3 { s3 =>
      val total =
        s3.getObjectMetadata(bucket, key).unsafeRunSync().get.contentLength
      val offset = total - tailBytes
      val oracle = downloadRangeToFile(s3, offset)
      val streamed = s3
        .readFileMultipart(
          bucket,
          key,
          partSize = 1,
          multiPartConcurrency = 4,
          fromOffset = offset
        )
        .compile
        .to(ByteVector)
        .unsafeRunSync()
      assert(oracle.length == tailBytes)
      assert(streamed == oracle)
    }
  }

  ignore("readFileMultipart throughput matrix") {
    info(s"Region: $region")

    val client = anonymousClient(region)
    try {
      val s3 = new S3(client, None)

      val configs = List(
        Config(5, 1),
        Config(5, 8),
        Config(5, 16),
        Config(5, 32),
        Config(2, 16),
        Config(2, 32),
        Config(2, 64),
        Config(1, 32),
        Config(1, 64),
        Config(8, 10),
      )

      info(f"${"partSize"}%10s ${"concurrency"}%12s ${"time(ms)"}%10s ${"MB/s"}%10s")
      info("-" * 48)

      var expectedSize = -1L
      for (cfg <- configs) {
        val (bytes, ms) = benchmark(s3, cfg)
        if (expectedSize < 0) expectedSize = bytes
        else assert(bytes == expectedSize, s"Size mismatch for $cfg")
        val sizeMB = bytes / (1024.0 * 1024.0)
        val throughput = sizeMB / ms * 1000
        println(f"${cfg.partSizeMB}%10d ${cfg.concurrency}%12d ${ms}%10d ${throughput}%10.1f")
        info(
          f"${cfg.partSizeMB}%10d ${cfg.concurrency}%12d ${ms}%10d ${throughput}%10.1f"
        )
      }
    } finally client.close()
  }
}
