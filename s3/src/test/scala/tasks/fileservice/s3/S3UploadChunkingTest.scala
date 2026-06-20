package tasks.fileservice.s3

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.{Chunk, Stream}
import org.scalatest.funsuite.AnyFunSuite

class S3UploadChunkingTest extends AnyFunSuite {

  private val mb = 1048576

  test("partSizeForIndex keeps the initial size within the first interval") {
    val initial = 5 * mb
    for (i <- Seq(1L, 2L, 500L, S3.partSizeDoublingInterval.toLong)) {
      assert(S3.partSizeForIndex(i, initial) == initial)
    }
  }

  test("partSizeForIndex doubles every doublingInterval parts") {
    val initial = 5 * mb
    val tier1 = S3.partSizeDoublingInterval + 1
    val tier2 = 2L * S3.partSizeDoublingInterval + 1
    val tier3 = 3L * S3.partSizeDoublingInterval + 1
    assert(S3.partSizeForIndex(tier1.toLong, initial) == initial * 2)
    assert(S3.partSizeForIndex(tier2, initial) == initial * 4)
    assert(S3.partSizeForIndex(tier3, initial) == initial * 8)
  }

  test("partSizeForIndex caps at maxPartSizeBytes") {
    val initial = 100 * mb
    assert(S3.partSizeForIndex(10000L, initial) == S3.maxPartSizeBytes)
  }

  test("growingChunks preserves byte content and order") {
    val total = 12 * mb + 137
    val input = Array.tabulate(total)(i => (i % 251).toByte)
    val chunks = Stream
      .chunk(Chunk.array(input))
      .covary[IO]
      .through(S3.growingChunks(5 * mb))
      .compile
      .toList
      .unsafeRunSync()
    val joined = chunks.flatMap(_._1.toArray.toSeq).toArray
    assert(joined.length == total)
    assert(joined.sameElements(input))
  }

  test("growingChunks numbers parts sequentially from 1") {
    val total = 11 * mb
    val input = new Array[Byte](total)
    val parts = Stream
      .chunk(Chunk.array(input))
      .covary[IO]
      .through(S3.growingChunks(5 * mb))
      .compile
      .toList
      .unsafeRunSync()
    assert(parts.map(_._2) == List(1L, 2L, 3L))
  }

  test("growingChunks emits empty output for an empty stream") {
    val parts = Stream.empty
      .covary[IO]
      .through(S3.growingChunks(5 * mb))
      .compile
      .toList
      .unsafeRunSync()
    assert(parts.isEmpty)
  }

  test(
    "growing chunk schedule supports objects up to S3's 5 TiB ceiling within 10000 parts"
  ) {
    val initial = 5 * mb
    val capacityBytes = (1 to 10000).map { i =>
      S3.partSizeForIndex(i.toLong, initial).toLong
    }.sum
    val fiveTiB = 5L * 1024L * 1024L * 1024L * 1024L
    assert(capacityBytes >= fiveTiB)
  }

  test(
    "growingChunks emits the rolled-over chunk when buffer overshoots target"
  ) {
    val initial = 5 * mb
    val source = Array.fill(initial * 3 + 7)(7.toByte)
    val sizes = Stream
      .chunk(Chunk.array(source))
      .covary[IO]
      .through(S3.growingChunks(initial))
      .map { case (c, _) => c.size }
      .compile
      .toList
      .unsafeRunSync()
    assert(sizes.sum == source.length)
    assert(sizes.dropRight(1).forall(_ == initial))
    assert(sizes.last == 7)
  }

}
