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
import org.ekrich.config.ConfigFactory

import java.io._

import org.scalatest.matchers.should.Matchers

import tasks.queue._
import tasks.fileservice._
import tasks.util._
import tasks.fileservice.proxy.ProxyFileStorage
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import tasks.util.config.TasksConfig
import org.http4s.ember.server.EmberServerBuilder
import com.comcast.ip4s._

object Conf {
  val str = """ """
}

class FileServiceSpec
    extends FunSpecLike
    with Matchers
    with BeforeAndAfterAll
    with TestHelpers {
  self: Suite =>

  implicit val sh: StreamHelper = new StreamHelper(None, None)

  implicit val tconfig: TasksConfig = tasks.util.config
    .parse(() => ConfigFactory.load())

  val remoteStore = new RemoteFileStorage

  override def afterAll() = {
    Thread.sleep(1500)

  }

  def tempFolder() = {
    val t = TempFile.createTempFile("fileservicespec")
    t.delete
    assert(t.mkdir)
    t
  }

  implicit val prefix: FileServicePrefix = FileServicePrefix(Vector())

  describe("fileservice new file folderstorage ") {
    it("add new empty file from source") {
      val data = Array[Byte]()
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input, data)

      val folder = tempFolder()

      val fs = new FolderFileStorage(folder)

      implicit val serviceimpl: FileServiceComponent =
        FileServiceComponent(fs, remoteStore)
      implicit val historyContext: HistoryContext = tasks.fileservice.NoHistory
      await(
        SharedFileHelper.createFromStream(
          fs2.Stream[IO, Byte](),
          "proba"
        )
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
      writeBinaryToFile(input, data)

      val folder = tempFolder()

      val fs = new FolderFileStorage(folder)

      implicit val serviceimpl: FileServiceComponent =
        FileServiceComponent(fs, remoteStore)
      implicit val historyContext = tasks.fileservice.NoHistory

      SharedFileHelper.createFromFile(input, "proba", false).unsafeRunSync()

      println(
        readBinaryFile(
          new java.io.File(folder, "proba").getCanonicalPath
        ).toVector
      )
      readBinaryFile(
        new java.io.File(folder, "proba").getCanonicalPath
      ).toVector should equal(
        data.toVector
      )
    }
    it("add new file - encrypted") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input, data)

      val folder = tempFolder()

      val fs = new EncryptedManagedFileStorage(
        new FolderFileStorage(folder),
        "00".repeat(32)
      )

      implicit val serviceimpl: FileServiceComponent =
        FileServiceComponent(fs, remoteStore)
      implicit val historyContext = tasks.fileservice.NoHistory

      val t =
        SharedFileHelper.createFromFile(input, "proba", false).unsafeRunSync()

      readBinaryFile(
        new java.io.File(folder, "proba").getCanonicalPath
      ).toVector should not equal (
        data.toVector
      )

      implicit val nlc =
        NodeLocalCache.start.unsafeRunSync()(
          cats.effect.unsafe.implicits.global
        )

      val path =
        Await.result(
          SharedFileHelper
            .getPathToFile(t)
            .allocated
            .map(_._1)
            .unsafeToFuture()(cats.effect.unsafe.implicits.global),
          50 seconds
        )

      readBinaryFile(path.getCanonicalPath).toVector should equal(data.toVector)
    }
    it("add new file") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input, data)

      val folder = tempFolder()

      val fs = new FolderFileStorage(folder)

      implicit val serviceimpl: FileServiceComponent =
        FileServiceComponent(fs, remoteStore)
      implicit val historyContext = tasks.fileservice.NoHistory

      SharedFileHelper.createFromFile(input, "proba", false).unsafeRunSync()

      readBinaryFile(
        new java.io.File(folder, "proba").getCanonicalPath
      ).toVector should equal(
        data.toVector
      )
    }
    it("add new file - not managed") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input, data)

      val folder = tempFolder()

      val fs = new FolderFileStorage(folder)

      val proxiedFs = ProxyFileStorage.service(
        fs
      )
      EmberServerBuilder
        .default[IO]
        .withHost(ipv4"0.0.0.0")
        .withHttpApp(proxiedFs.orNotFound)
        .build
        .use { server =>
          ProxyFileStorage.makeClient(server.baseUri).use { proxiedFs =>
            IO {
              implicit val serviceimpl: FileServiceComponent =
                FileServiceComponent(proxiedFs, remoteStore)
              implicit val historyContext = tasks.fileservice.NoHistory

              SharedFileHelper
                .createFromFile(input, "proba", false)
                .unsafeRunSync()

              readBinaryFile(
                new java.io.File(folder, "proba").getCanonicalPath
              ).toVector should equal(
                data.toVector
              )
            }

          }
        }
        .unsafeRunSync()
    }
    it("add new file - from source") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input, data)

      val folder = tempFolder()

      val fs = new FolderFileStorage(folder)

      implicit val serviceimpl: FileServiceComponent =
        FileServiceComponent(fs, remoteStore)
      implicit val historyContext = tasks.fileservice.NoHistory
      await(
        SharedFileHelper.createFromStream(
          fs2.Stream.chunk(fs2.Chunk.array(data)),
          "proba"
        )
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
      writeBinaryToFile(input, data)

      val folder = tempFolder()

      val fs = new FolderFileStorage(folder)

      implicit val serviceimpl: FileServiceComponent =
        FileServiceComponent(fs, remoteStore)
      implicit val historyContext = tasks.fileservice.NoHistory
      await(
        SharedFileHelper.createFromStream(
          fs2.Stream.chunk(fs2.Chunk.array(data)),
          "proba"
        )
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
      writeBinaryToFile(input, data)

      val folder = tempFolder()
      val fs = new FolderFileStorage(folder)

      implicit val serviceimpl: FileServiceComponent =
        FileServiceComponent(fs, remoteStore)
      implicit val nlc =
        NodeLocalCache.start.unsafeRunSync()(
          cats.effect.unsafe.implicits.global
        )
      implicit val historyContext = tasks.fileservice.NoHistory
      val t: SharedFile =
        await(
          SharedFileHelper.createFromFile(input, "proba", false)
        )

      readBinaryFile(
        new java.io.File(folder, "proba").getCanonicalPath
      ).toVector should equal(
        data.toVector
      )

      val path =
        Await.result(
          SharedFileHelper
            .getPathToFile(t)
            .allocated
            .map(_._1)
            .unsafeToFuture()(cats.effect.unsafe.implicits.global),
          50 seconds
        )

      readBinaryFile(path.getCanonicalPath).toVector should equal(data.toVector)

    }
    it("folder file storage does not delete non-temp files") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input, data)

      val folder = tempFolder()
      val fs = new FolderFileStorage(folder)

      implicit val serviceimpl: FileServiceComponent =
        FileServiceComponent(fs, remoteStore)
      implicit val nlc =
        NodeLocalCache.start.unsafeRunSync()(
          cats.effect.unsafe.implicits.global
        )
      implicit val historyContext = tasks.fileservice.NoHistory
      val t: SharedFile =
        await(
          SharedFileHelper.createFromFile(input, "proba", false)
        )

      readBinaryFile(
        new java.io.File(folder, "proba").getCanonicalPath
      ).toVector should equal(
        data.toVector
      )

      SharedFileHelper
        .getPathToFile(t)
        .use { f =>
          IO {
            readBinaryFile(f.getCanonicalPath).toVector should equal(
              data.toVector
            )
          }
        }
        .timeout(60 seconds)
        .unsafeRunSync()(cats.effect.unsafe.implicits.global)

      readBinaryFile(
        new java.io.File(folder, "proba").getCanonicalPath
      ).toVector should equal(
        data.toVector
      )

    }
    it("add new file and ask for it - not managed") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input, data)

      val folder = tempFolder()
      val fs = new FolderFileStorage(folder)

      implicit val serviceimpl: FileServiceComponent =
        FileServiceComponent(fs, remoteStore)
      implicit val nlc =
        NodeLocalCache.start.unsafeRunSync()(
          cats.effect.unsafe.implicits.global
        )
      implicit val historyContext = tasks.fileservice.NoHistory
      val t: SharedFile =
        await(
          SharedFileHelper.createFromFile(input, "proba", false)
        )

      readBinaryFile(
        new java.io.File(folder, "proba").getCanonicalPath
      ).toVector should equal(
        data.toVector
      )

      val path = Await.result(
        SharedFileHelper
          .getPathToFile(t)
          .allocated
          .map(_._1)
          .unsafeToFuture()(cats.effect.unsafe.implicits.global),
        50 seconds
      )
      readBinaryFile(path.getCanonicalPath).toVector should equal(data.toVector)

      SharedFileHelper
        .stream(t, 0L)
        .compile
        .foldChunks(fs2.Chunk.empty[Byte])(_ ++ _)
        .unsafeRunSync()
        .toArray
        .toVector should equal(data.toVector)

    }

    it("add new file and stream it") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input, data)

      val folder = tempFolder()
      val fs = new FolderFileStorage(folder)

      implicit val serviceimpl: FileServiceComponent =
        FileServiceComponent(fs, remoteStore)

      implicit val historyContext = tasks.fileservice.NoHistory
      val t: SharedFile =
        await(
          SharedFileHelper.createFromFile(input, "proba", false)
        )

      readBinaryFile(
        new java.io.File(folder, "proba").getCanonicalPath
      ).toVector should equal(
        data.toVector
      )

      val content =
        SharedFileHelper
          .stream(t, 0L)
          .compile
          .foldChunks(fs2.Chunk.empty[Byte])(_ ++ _)
          .unsafeRunSync()

      content.toVector should equal(data.toVector)

    }
    it("add new file and source it- not managed") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input, data)

      val folder = tempFolder()
      val fs = new FolderFileStorage(folder)
      // val proxiedFs = new ActorFileStorage(
      //   system.actorOf(
      //     Props(new FileServiceProxy(fs))
      //   )
      // )

      implicit val serviceimpl: FileServiceComponent =
        FileServiceComponent(fs, remoteStore)
      implicit val nlc =
        NodeLocalCache.start.unsafeRunSync()(
          cats.effect.unsafe.implicits.global
        )
      implicit val historyContext = tasks.fileservice.NoHistory
      val t: SharedFile =
        await(
          SharedFileHelper.createFromFile(input, "proba", false)
        )

      readBinaryFile(
        new java.io.File(folder, "proba").getCanonicalPath
      ).toVector should equal(
        data.toVector
      )

      val path = Await.result(
        SharedFileHelper
          .getPathToFile(t)
          .allocated
          .map(_._1)
          .unsafeToFuture()(cats.effect.unsafe.implicits.global),
        50 seconds
      )
      readBinaryFile(path.getCanonicalPath).toVector should equal(data.toVector)

      SharedFileHelper
        .stream(t, 0L)
        .compile
        .foldChunks(fs2.Chunk.empty[Byte])(_ ++ _)
        .unsafeRunSync()
        .toArray
        .toVector should equal(data.toVector)

      SharedFileHelper
        .stream(t, 2L)
        .compile
        .foldChunks(fs2.Chunk.empty[Byte])(_ ++ _)
        .unsafeRunSync()
        .toArray
        .toVector should equal(data.toVector.drop(2))

    }

    it("after cache restart") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)

      val folder = tempFolder()
      val input = new java.io.File(folder, "proba")
      writeBinaryToFile(input, data)
      val fs = new FolderFileStorage(folder)
      implicit val serviceimpl: FileServiceComponent =
        FileServiceComponent(fs, remoteStore)
      implicit val nlc =
        NodeLocalCache.start.unsafeRunSync()(
          cats.effect.unsafe.implicits.global
        )

      val t: SharedFile = SharedFileHelper.createForTesting(
        "proba",
        16,
        fs2.Stream
          .chunk(fs2.Chunk.array(data))
          .through(fs2.hashing.Hashing[IO].hash(fs2.hashing.HashAlgorithm.MD5))
          .compile
          .lastOrError
          .map(_.bytes.take(4).toByteBuffer.asIntBuffer().get)
          .unsafeRunSync()
      )

      val path = Await.result(
        SharedFileHelper
          .getPathToFile(t)
          .allocated
          .map(_._1)
          .unsafeToFuture()(cats.effect.unsafe.implicits.global),
        50 seconds
      )
      readBinaryFile(path.getCanonicalPath).toVector should equal(data.toVector)

      await(SharedFileHelper.isAccessible(t, true)) should be(
        true
      )

    }
  }

  describe("fileservice with centralized storage with simulated remote") {

    it("after cache restart") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)

      val folder = tempFolder()
      val input = new java.io.File(folder, "proba")
      writeBinaryToFile(input, data)
      val fs = new FolderFileStorage(folder)
      // val proxiedFs = new ActorFileStorage(
      //   system.actorOf(
      //     Props(new FileServiceProxy(fs))
      //   )
      // )

      implicit val serviceimpl: FileServiceComponent =
        FileServiceComponent(fs, remoteStore)
      implicit val nlc =
        NodeLocalCache.start.unsafeRunSync()(
          cats.effect.unsafe.implicits.global
        )

      val t: SharedFile = SharedFileHelper.createForTesting(
        "proba",
        16,
        fs2.Stream
          .chunk(fs2.Chunk.array(data))
          .through(fs2.hashing.Hashing[IO].hash(fs2.hashing.HashAlgorithm.MD5))
          .compile
          .lastOrError
          .map(_.bytes.take(4).toByteBuffer.asIntBuffer().get)
          .unsafeRunSync()
      )

      val path = Await.result(
        SharedFileHelper
          .getPathToFile(t)
          .allocated
          .map(_._1)
          .unsafeToFuture()(cats.effect.unsafe.implicits.global),
        30 seconds
      )
      readBinaryFile(path.getCanonicalPath).toVector should equal(data.toVector)

    }
  }

}
