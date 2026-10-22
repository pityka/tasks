package tasks.util

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import tasks.util.transactional.TransactionalStateDiff
import tasks.util.transactional.TransactionalStateDiff._

class TransactionalStateDiffTest extends FunSuite with Matchers {

  test("diffCollection returns no ops when old and new are both empty") {
    val ops =
      TransactionalStateDiff.diffCollection[String](
        oldItems = Map.empty,
        newItems = Map.empty,
        baseVersions = Map.empty
      )
    ops shouldBe empty
  }

  test(
    "diffCollection returns no ops when old and new are equal (default equality)"
  ) {
    val items = Map("a" -> "A", "b" -> "B")
    val ops =
      TransactionalStateDiff.diffCollection[String](items, items, Map.empty)
    ops shouldBe empty
  }

  test("diffCollection emits an Insert for each new key, with no baseVersion") {
    val ops = TransactionalStateDiff.diffCollection[String](
      oldItems = Map.empty,
      newItems = Map("b" -> "B", "a" -> "A"),
      baseVersions = Map.empty
    )
    ops shouldBe List(
      Insert("a", "A"),
      Insert("b", "B")
    )
  }

  test(
    "diffCollection emits a Delete carrying the base version for each removed key"
  ) {
    val ops = TransactionalStateDiff.diffCollection[String](
      oldItems = Map("a" -> "A", "b" -> "B"),
      newItems = Map("a" -> "A"),
      baseVersions = Map("a" -> 3L, "b" -> 7L)
    )
    ops shouldBe List(Delete("b", 7L))
  }

  test(
    "diffCollection emits an Update carrying the base version for each changed key"
  ) {
    val ops = TransactionalStateDiff.diffCollection[String](
      oldItems = Map("a" -> "A", "b" -> "B"),
      newItems = Map("a" -> "A!", "b" -> "B"),
      baseVersions = Map("a" -> 5L)
    )
    ops shouldBe List(Update("a", "A!", 5L))
  }

  test("diffCollection respects a custom equality (e.g. Array[Byte])") {
    val oldItems = Map("k" -> Array[Byte](1, 2, 3))
    val newItems = Map("k" -> Array[Byte](1, 2, 3))
    val defaultOps = TransactionalStateDiff.diffCollection[Array[Byte]](
      oldItems,
      newItems,
      Map("k" -> 1L)
    )
    withClue(
      "default equality uses reference equality on arrays, so it should see a change"
    ) {
      defaultOps should not be empty
    }
    val customOps = TransactionalStateDiff.diffCollection[Array[Byte]](
      oldItems,
      newItems,
      Map("k" -> 1L),
      equal = (a: Array[Byte], b: Array[Byte]) => java.util.Arrays.equals(a, b)
    )
    customOps shouldBe empty
  }

  test("diffCollection returns ops ordered by key") {
    val ops = TransactionalStateDiff.diffCollection[String](
      oldItems = Map("c" -> "C", "b" -> "B"),
      newItems = Map("a" -> "A", "d" -> "D"),
      baseVersions = Map("c" -> 1L, "b" -> 2L)
    )
    ops.map(_.key) shouldBe ops.map(_.key).sorted
  }

  test(
    "diffCollection defaults the base version of a missing key to 0"
  ) {
    val ops = TransactionalStateDiff.diffCollection[String](
      oldItems = Map("a" -> "A"),
      newItems = Map.empty,
      baseVersions = Map.empty
    )
    ops shouldBe List(Delete("a", 0L))
  }

  test("diffSingleton returns None when old and new are equal") {
    TransactionalStateDiff.diffSingleton(
      key = "res",
      oldItem = "a",
      newItem = "a",
      baseVersion = 42L
    ) shouldBe None
  }

  test(
    "diffSingleton returns a single Update at baseVersion when values differ"
  ) {
    TransactionalStateDiff.diffSingleton(
      key = "res",
      oldItem = "a",
      newItem = "b",
      baseVersion = 42L
    ) shouldBe Some(Update("res", "b", 42L))
  }

  test("packUnits returns an empty list of chunks for no units") {
    TransactionalStateDiff.packUnits[String](
      units = Nil,
      maxOpsPerChunk = 10,
      maxBytesPerChunk = 100
    ) shouldBe empty
  }

  test("packUnits packs several small units into a single chunk") {
    val units =
      (0 until 5).toList.map(i => WriteUnit[String](List(s"op-$i"), 1))
    val chunks = TransactionalStateDiff.packUnits(
      units,
      maxOpsPerChunk = 10,
      maxBytesPerChunk = 100
    )
    chunks.size shouldBe 1
    chunks.head.size shouldBe 5
  }

  test(
    "packUnits splits by op count when a chunk would exceed maxOpsPerChunk"
  ) {
    val units =
      (0 until 7).toList.map(i => WriteUnit[String](List(s"op-$i"), 1))
    val chunks = TransactionalStateDiff.packUnits(
      units,
      maxOpsPerChunk = 3,
      maxBytesPerChunk = 100
    )
    chunks.size shouldBe 3
    chunks.map(_.size) shouldBe List(3, 3, 1)
  }

  test(
    "packUnits splits by byte count when a chunk would exceed maxBytesPerChunk"
  ) {
    val units =
      (0 until 4).toList.map(i => WriteUnit[String](List(s"op-$i"), 30))
    val chunks = TransactionalStateDiff.packUnits(
      units,
      maxOpsPerChunk = 100,
      maxBytesPerChunk = 50
    )
    chunks.size shouldBe 4
    all(chunks.map(_.size)) shouldBe 1
  }

  test(
    "packUnits never splits a WriteUnit (ops of one unit stay in the same chunk)"
  ) {
    val units = List(
      WriteUnit[String](List("a1", "a2", "a3"), 30),
      WriteUnit[String](List("b1", "b2"), 20)
    )
    val chunks = TransactionalStateDiff.packUnits(
      units,
      maxOpsPerChunk = 4,
      maxBytesPerChunk = 100
    )
    chunks.size shouldBe 2
    chunks.head shouldBe List("a1", "a2", "a3")
    chunks.last shouldBe List("b1", "b2")
  }

  test(
    "packUnits throws OversizedUnitException with the caller-supplied message"
  ) {
    val oversized = WriteUnit[String](List("big"), 200)
    val thrown = intercept[TransactionalStateDiff.OversizedUnitException] {
      TransactionalStateDiff.packUnits(
        units = List(oversized),
        maxOpsPerChunk = 10,
        maxBytesPerChunk = 100,
        describeOversized = (u: WriteUnit[String]) => s"nope: ${u.bytes} bytes"
      )
    }
    thrown.getMessage shouldBe "nope: 200 bytes"
  }

  test(
    "withSingletonBarrier returns Nil when there are no chunks and no terminal op"
  ) {
    TransactionalStateDiff.withSingletonBarrier[String](
      dataChunks = Nil,
      barrier = "B",
      terminalSingletonOp = None
    ) shouldBe Nil
  }

  test(
    "withSingletonBarrier returns a single chunk with just the terminal op when there are no data chunks"
  ) {
    TransactionalStateDiff.withSingletonBarrier[String](
      dataChunks = Nil,
      barrier = "B",
      terminalSingletonOp = Some("T")
    ) shouldBe List(List("T"))
  }

  test(
    "withSingletonBarrier appends the barrier to every chunk when there is no terminal op"
  ) {
    TransactionalStateDiff.withSingletonBarrier[String](
      dataChunks = List(List("a1"), List("b1", "b2")),
      barrier = "B",
      terminalSingletonOp = None
    ) shouldBe List(List("a1", "B"), List("b1", "b2", "B"))
  }

  test(
    "withSingletonBarrier appends the barrier to every chunk except the last, which gets the terminal op"
  ) {
    TransactionalStateDiff.withSingletonBarrier[String](
      dataChunks = List(List("a1"), List("b1"), List("c1")),
      barrier = "B",
      terminalSingletonOp = Some("T")
    ) shouldBe List(List("a1", "B"), List("b1", "B"), List("c1", "T"))
  }

}
