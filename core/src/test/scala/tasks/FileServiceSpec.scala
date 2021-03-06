/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Modified work, Copyright (c) 2016 Istvan Bartha

 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package tasks

import org.scalatest._
import org.scalatest.funspec.{AnyFunSpecLike => FunSpecLike}
import scala.concurrent.duration._
import scala.concurrent._
import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import akka.actor.{Props, ActorSystem}
import com.typesafe.config.ConfigFactory

import java.io._

import org.scalatest.matchers.should.Matchers

import tasks.queue._
import tasks.fileservice._
import tasks.util._

object Conf {
  val str = """my-pinned-dispatcher {
  executor = "thread-pool-executor"
  type = PinnedDispatcher
  thread-pool-executor.allow-core-timeout=off
}
akka.loglevel = "OFF" """
}

class FileServiceSpec
    extends TestKit(
      ActorSystem(
        "testsystem",
        ConfigFactory
          .parseString(Conf.str)
          .withFallback(ConfigFactory.load("akka.conf"))
      )
    )
    with ImplicitSender
    with FunSpecLike
    with Matchers
    with BeforeAndAfterAll {
  self: Suite =>

  val as = implicitly[ActorSystem]
  import as.dispatcher
  // implicit val s3stream = new S3StreamQueued(AWSCredentials("na", "na"), "na")
  implicit val sh = new StreamHelper(None)

  implicit val tconfig = tasks.util.config
    .parse(() =>
      ConfigFactory.load().withFallback(ConfigFactory.load("akka.conf"))
    )

  val remoteStore = new RemoteFileStorage

  override def afterAll() = {
    Thread.sleep(1500)
    system.terminate()

  }

  implicit val prefix = FileServicePrefix(Vector())

  describe("fileservice new file folderstorage ") {
    it("add new empty file from source") {
      val data = Array[Byte]()
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input.getCanonicalPath, data)

      val folder =
        new File(
          new java.io.File(getClass.getResource("/").getPath),
          "test-empty-source"
        )
      folder.mkdir
      val folder2 =
        new File(
          new java.io.File(getClass.getResource("/").getPath),
          "test-empty-sourcef"
        )
      folder2.mkdir

      val fs = new FolderFileStorage(folder)
      val service = system.actorOf(
        Props(
          new FileService(
            fs
          )
        )
      )
      implicit val serviceimpl =
        FileServiceComponent(service, Some(fs), remoteStore)
      implicit val historyContext = tasks.fileservice.NoHistory
      Await.result(
        SharedFileHelper.createFromSource(
          akka.stream.scaladsl.Source(List.empty[akka.util.ByteString]),
          "proba"
        ),
        30 seconds
      )

      readBinaryFile(
        new java.io.File(folder, "proba").getCanonicalPath
      ).toVector should equal(
        data.toVector
      )
    }

    it("add new empty file") {
      val data = Array[Byte]()
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input.getCanonicalPath, data)

      val folder =
        new File(new java.io.File(getClass.getResource("/").getPath), "test1")
      folder.mkdir
      val folder2 =
        new File(new java.io.File(getClass.getResource("/").getPath), "test1f")
      folder2.mkdir

      val fs = new FolderFileStorage(folder)
      val service = system.actorOf(
        Props(
          new FileService(
            fs
          )
        )
      )
      implicit val serviceimpl =
        FileServiceComponent(service, Some(fs), remoteStore)
      implicit val historyContext = tasks.fileservice.NoHistory
      scala.concurrent.Await.result(
        SharedFileHelper.createFromFile(input, "proba", false),
        atMost = Duration.Inf
      )

      readBinaryFile(
        new java.io.File(folder, "proba").getCanonicalPath
      ).toVector should equal(
        data.toVector
      )
    }
    it("add new file") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input.getCanonicalPath, data)

      val folder =
        new File(new java.io.File(getClass.getResource("/").getPath), "test1")
      folder.mkdir
      val folder2 =
        new File(new java.io.File(getClass.getResource("/").getPath), "test1f")
      folder2.mkdir

      val fs = new FolderFileStorage(folder)
      val service = system.actorOf(
        Props(
          new FileService(
            fs
          )
        )
      )
      implicit val serviceimpl =
        FileServiceComponent(service, Some(fs), remoteStore)
      implicit val historyContext = tasks.fileservice.NoHistory
      SharedFileHelper.createFromFile(input, "proba", false)

      readBinaryFile(
        new java.io.File(folder, "proba").getCanonicalPath
      ).toVector should equal(
        data.toVector
      )
    }
    it("add new file - not managed") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input.getCanonicalPath, data)

      val folder =
        new File(new java.io.File(getClass.getResource("/").getPath), "test1")
      folder.mkdir
      val folder2 =
        new File(new java.io.File(getClass.getResource("/").getPath), "test1f")
      folder2.mkdir

      val fs = new FolderFileStorage(folder)
      val service = system.actorOf(
        Props(
          new FileService(
            fs
          )
        )
      )
      implicit val serviceimpl =
        FileServiceComponent(service, None, remoteStore)
      implicit val historyContext = tasks.fileservice.NoHistory
      SharedFileHelper.createFromFile(input, "proba", false)

      readBinaryFile(
        new java.io.File(folder, "proba").getCanonicalPath
      ).toVector should equal(
        data.toVector
      )
    }
    it("add new file - from source") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input.getCanonicalPath, data)

      val folder =
        new File(
          new java.io.File(getClass.getResource("/").getPath),
          "test1-from-source"
        )
      folder.mkdir
      val folder2 =
        new File(
          new java.io.File(getClass.getResource("/").getPath),
          "test1f-fro-source"
        )
      folder2.mkdir

      val fs = new FolderFileStorage(folder)
      val service = system.actorOf(
        Props(
          new FileService(
            fs
          )
        )
      )
      implicit val serviceimpl =
        FileServiceComponent(service, Some(fs), remoteStore)
      implicit val historyContext = tasks.fileservice.NoHistory
      Await.result(
        SharedFileHelper.createFromSource(
          akka.stream.scaladsl.Source.single(akka.util.ByteString(data)),
          "proba"
        ),
        30 seconds
      )

      readBinaryFile(
        new java.io.File(folder, "proba").getCanonicalPath
      ).toVector should equal(
        data.toVector
      )
    }
    it("add new file - from source - not managed") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input.getCanonicalPath, data)

      val folder =
        new File(
          new java.io.File(getClass.getResource("/").getPath),
          "test1-from-source"
        )
      folder.mkdir
      val folder2 =
        new File(
          new java.io.File(getClass.getResource("/").getPath),
          "test1f-fro-source"
        )
      folder2.mkdir

      val fs = new FolderFileStorage(folder)
      val service = system.actorOf(
        Props(
          new FileService(
            fs
          )
        )
      )
      implicit val serviceimpl =
        FileServiceComponent(service, None, remoteStore)
      implicit val historyContext = tasks.fileservice.NoHistory
      Await.result(
        SharedFileHelper.createFromSource(
          akka.stream.scaladsl.Source.single(akka.util.ByteString(data)),
          "proba"
        ),
        30 seconds
      )

      readBinaryFile(
        new java.io.File(folder, "proba").getCanonicalPath
      ).toVector should equal(
        data.toVector
      )
    }

    it("add new file and ask for it") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input.getCanonicalPath, data)

      val folder =
        new File(new java.io.File(getClass.getResource("/").getPath), "test2")
      folder.mkdir
      val folder2 =
        new File(new java.io.File(getClass.getResource("/").getPath), "test2f")
      folder2.mkdir
      val fs = new FolderFileStorage(folder)
      val service = system.actorOf(
        Props(
          new FileService(
            fs
          )
        )
      )
      implicit val serviceimpl =
        FileServiceComponent(service, Some(fs), remoteStore)
      implicit val nlc =
        NodeLocalCache.start
      implicit val historyContext = tasks.fileservice.NoHistory
      val t: SharedFile =
        Await.result(
          SharedFileHelper.createFromFile(input, "proba", false),
          50 seconds
        )

      readBinaryFile(
        new java.io.File(folder, "proba").getCanonicalPath
      ).toVector should equal(
        data.toVector
      )

      val path = Await.result(SharedFileHelper.getPathToFile(t), 50 seconds)
      readBinaryFile(path.getCanonicalPath).toVector should equal(data.toVector)

    }
    it("add new file and ask for it - not managed") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input.getCanonicalPath, data)

      val folder =
        new File(new java.io.File(getClass.getResource("/").getPath), "test2")
      folder.mkdir
      val folder2 =
        new File(new java.io.File(getClass.getResource("/").getPath), "test2f")
      folder2.mkdir
      val fs = new FolderFileStorage(folder)
      val service = system.actorOf(
        Props(
          new FileService(
            fs,
            isLocal = _ => false
          )
        )
      )
      implicit val serviceimpl =
        FileServiceComponent(service, None, remoteStore)
      implicit val nlc =
        NodeLocalCache.start
      implicit val historyContext = tasks.fileservice.NoHistory
      val t: SharedFile =
        Await.result(
          SharedFileHelper.createFromFile(input, "proba", false),
          50 seconds
        )

      readBinaryFile(
        new java.io.File(folder, "proba").getCanonicalPath
      ).toVector should equal(
        data.toVector
      )

      val path = Await.result(SharedFileHelper.getPathToFile(t), 50 seconds)
      readBinaryFile(path.getCanonicalPath).toVector should equal(data.toVector)

      Await
        .result(
          SharedFileHelper
            .getSourceToFile(t, 0L)
            .runFold(akka.util.ByteString.empty)(_ ++ _),
          20 seconds
        )
        .toArray
        .toVector should equal(data.toVector)

    }

    it("after cache restart") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)

      val folder =
        new File(new java.io.File(getClass.getResource("/").getPath), "test3")
      folder.mkdir
      val folder2 =
        new File(new java.io.File(getClass.getResource("/").getPath), "test3f")
      folder2.mkdir
      val input = new java.io.File(folder, "proba")
      writeBinaryToFile(input.getCanonicalPath, data)
      val fs = new FolderFileStorage(folder)
      val service = system.actorOf(Props(new FileService(fs)))
      implicit val serviceimpl =
        FileServiceComponent(service, Some(fs), remoteStore)
      implicit val nlc =
        NodeLocalCache.start

      val t: SharedFile = SharedFileHelper.createForTesting(
        "proba",
        16,
        com.google.common.hash.Hashing.crc32c.hashBytes(data).asInt
      )

      val path = Await.result(SharedFileHelper.getPathToFile(t), 50 seconds)
      readBinaryFile(path.getCanonicalPath).toVector should equal(data.toVector)

      Await.result(
        SharedFileHelper.isAccessible(t, true),
        30 seconds
      ) should be(
        true
      )

    }
  }

  describe("fileservice with centralized storage with simulated remote") {

    it("after cache restart") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)

      val folder =
        new File(new java.io.File(getClass.getResource("/").getPath), "test9")
      folder.mkdir
      val folder2 =
        new File(new java.io.File(getClass.getResource("/").getPath), "test9f")
      folder2.mkdir
      val input = new java.io.File(folder, "proba")
      writeBinaryToFile(input.getCanonicalPath, data)
      val fs = new FolderFileStorage(folder)
      val service =
        system.actorOf(Props(new FileService(fs, 8, (_: File) => false)))
      implicit val serviceimpl =
        FileServiceComponent(service, None, remoteStore)
      implicit val nlc =
        NodeLocalCache.start

      val t: SharedFile = SharedFileHelper.createForTesting(
        "proba",
        16,
        com.google.common.hash.Hashing.crc32c.hashBytes(data).asInt
      )

      val path = Await.result(SharedFileHelper.getPathToFile(t), 30 seconds)
      readBinaryFile(path.getCanonicalPath).toVector should equal(data.toVector)

    }
  }

}
