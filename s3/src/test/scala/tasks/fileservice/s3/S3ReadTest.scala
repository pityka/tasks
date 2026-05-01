package tasks.fileservice.s3

import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient

class S3ReadTest extends AnyFunSuite {

  private def anonymousClient(region: Region): S3AsyncClient =
    S3AsyncClient
      .builder()
      .credentialsProvider(AnonymousCredentialsProvider.create())
      .region(region)
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

  test("readFileMultipart throughput matrix") {
    info(s"Region: $region")

    val client = anonymousClient(region)
    try {
      val s3 = new S3(client)

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
