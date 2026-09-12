package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import tasks.fileservice._
import tasks.util._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.ekrich.config.ConfigFactory

class CheckContentEqualityTestSuite
    extends FunSuite
    with Matchers
    with TestHelpers {

  implicit val tconfig: tasks.util.config.TasksConfig =
    tasks.util.config.parse(() =>
      ConfigFactory
        .parseString(
          "tasks.fileservice.folderFileStorageCompleteFileCheck = true"
        )
        .withFallback(ConfigFactory.load())
    )

  test(
    "re-importing identical file should not create .old.0 backup"
  ) {
    val folder = {
      val t = TempFile.createTempFile("equalitytest")
      t.delete
      assert(t.mkdir)
      t
    }

    val fs = new FolderFileStorage(folder)
    val data = "hello world".getBytes

    val source1 = TempFile.createTempFile(".src1")
    writeBinaryToFile(source1, data)

    val source2 = TempFile.createTempFile(".src2")
    writeBinaryToFile(source2, data)

    val proposed = ProposedManagedFilePath(Vector("test", "file.txt"))

    (for {
      _ <- fs.importFile(source1, proposed, canMove = false)
      _ <- fs.importFile(source2, proposed, canMove = false)
    } yield {
      // With the fix, checkContentEquality correctly detects identical files,
      // so no backup is created. With the bug, .old.0 would exist because
      // checkContentEquality wrongly returned false for non-empty identical files.
      val backup =
        new java.io.File(
          folder,
          "test" + java.io.File.separator + "file.txt.old.0"
        )
      backup.exists shouldBe false
    }).unsafeRunSync()
  }

  test(
    "re-importing different file should create .old.0 backup"
  ) {
    val folder = {
      val t = TempFile.createTempFile("inequalitytest")
      t.delete
      assert(t.mkdir)
      t
    }

    val fs = new FolderFileStorage(folder)

    val source1 = TempFile.createTempFile(".src1")
    writeBinaryToFile(source1, "content A".getBytes)

    val source2 = TempFile.createTempFile(".src2")
    writeBinaryToFile(source2, "content B".getBytes)

    val proposed = ProposedManagedFilePath(Vector("test", "file.txt"))

    (for {
      _ <- fs.importFile(source1, proposed, canMove = false)
      _ <- fs.importFile(source2, proposed, canMove = false)
    } yield {
      // Different content — old file should be renamed to .old.0
      val backup =
        new java.io.File(
          folder,
          "test" + java.io.File.separator + "file.txt.old.0"
        )
      backup.exists shouldBe true
    }).unsafeRunSync()
  }

}
