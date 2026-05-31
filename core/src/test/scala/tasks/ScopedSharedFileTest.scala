package tasks

import cats.effect.unsafe.implicits.global

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import tasks.jsonitersupport._
import tasks.fileservice.ManagedFilePath
import tasks.queue.{FilePrefix, Prefix}

object ScopedSharedFileTest extends TestHelpers {

  val task = Task[Input, SharedFile]("scopedsharedfile", 1) {
    input => implicit env =>
      val tmp = tasks.util.TempFile.createTempFile(".tmp")
      java.nio.file.Files
        .write(tmp.toPath, s"value=${input.i}".getBytes("UTF-8"))
      SharedFile.scoped(tmp, "out.txt")
  }

  def run = {
    withTaskSystem(testConfig) { implicit ts =>
      for {
        a <- task(Input(1))(ResourceRequest(1, 500))
        b <- task(Input(2))(ResourceRequest(1, 500))
      } yield (a, b)
    }
  }

}

object ScopedSharedFileCustomPrefixTest extends TestHelpers {

  implicit val customPrefix: FilePrefix[Input] = new FilePrefix[Input] {
    def prefix(a: Input): Prefix = Prefix(s"custom-${a.i}")
  }

  val task = Task[Input, SharedFile]("scopedsharedfilecustom", 1) {
    input => implicit env =>
      val tmp = tasks.util.TempFile.createTempFile(".tmp")
      java.nio.file.Files
        .write(tmp.toPath, s"value=${input.i}".getBytes("UTF-8"))
      SharedFile.scoped(tmp, "out.txt")
  }

  def run = {
    withTaskSystem(testConfig) { implicit ts =>
      task(Input(7))(ResourceRequest(1, 500))
    }
  }

}

class ScopedSharedFileTestSuite extends FunSuite with Matchers {

  test("SharedFile.scoped produces distinct paths for distinct task inputs") {
    val result = ScopedSharedFileTest.run.unsafeRunSync().toOption.get
    val (a, b) = result

    val pa = a.path.asInstanceOf[ManagedFilePath].pathElements
    val pb = b.path.asInstanceOf[ManagedFilePath].pathElements

    pa.last shouldBe "out.txt"
    pb.last shouldBe "out.txt"
    pa should not equal pb
    pa.dropRight(1) should not equal pb.dropRight(1)
  }

  test("A user-supplied FilePrefix overrides the on-disk path segment") {
    val sf = ScopedSharedFileCustomPrefixTest.run.unsafeRunSync().toOption.get
    val elements = sf.path.asInstanceOf[ManagedFilePath].pathElements
    elements.last shouldBe "out.txt"
    elements should contain("custom-7")
  }

}
