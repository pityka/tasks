/*
 * The MIT License
 *
 * Copyright (c) 2018 Istvan Bartha
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

package tasks.elastic

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}

import org.scalatest.matchers.should.Matchers
import cats.effect.unsafe.implicits.global

import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

import tasks.queue.UntypedResult
import tasks._
import com.typesafe.config.ConfigFactory
import cats.effect.IO

object SHResultWithSharedFilesTest extends TestHelpers {

  case class Intermediate(sf: SharedFile)
  case class IntermediateMutable(sf: SharedFile, mut: Option[SharedFile])
      extends WithSharedFiles(mutables = mut.toSeq)

  object Intermediate {
    implicit val codec: JsonValueCodec[Intermediate] = JsonCodecMaker.make

  }
  object IntermediateMutable {
    implicit val codec: JsonValueCodec[IntermediateMutable] =
      JsonCodecMaker.make

  }

  case class OtherCollection(sf: Intermediate)
  object OtherCollection {
    implicit val codec: JsonValueCodec[OtherCollection] = JsonCodecMaker.make

  }

  case class Output(
      sf1: SharedFile,
      sf2: SharedFile,
      mut: SharedFile,
      recursive: Intermediate,
      collection: Seq[Intermediate],
      collection2: Seq[Seq[Intermediate]],
      collection3: OtherCollection,
      collection3Unlisted: OtherCollection,
      collection3Mut: OtherCollection,
      recursiveMut: IntermediateMutable,
      collectionMut: Seq[IntermediateMutable],
      option1: Option[SharedFile],
      option2: Option[Intermediate],
      mut2: IntermediateMutable,
      map: Map[String, Intermediate]
  ) extends WithSharedFiles(
        members = List(collection3.sf),
        mutables = List(mut, mut2, collection3Mut)
      )

  object Output {
    implicit val codec: JsonValueCodec[Output] = JsonCodecMaker.make
  }

  val testTask =
    Task[(Input, SharedFile), Output]("resultwithsharedfilestest", 1) {
      case (_, inputsf) =>
        implicit computationEnvironment =>
          inputsf.file.allocated
            .map(_._1)
            .unsafeRunSync()(cats.effect.unsafe.implicits.global)
          val source = fs2.Stream.chunk(fs2.Chunk.array("abcd".getBytes()))
          val tmpfile = {
            val tmp = java.io.File.createTempFile("dsfsdf","dfs")
            tasks.util.writeBinaryToFile(tmp.getAbsolutePath,Array[Byte](1, 2, 3))
            tmp 
          }
          val fs = List(
            SharedFile(tmpfile, "f1"),
            SharedFile(source, "f2"),
            SharedFile(source, "f3"),
            SharedFile(source, "f4"),
            SharedFile(source, "f5"),
            SharedFile(source, "f6"),
            SharedFile(source, "f7"),
            SharedFile(source, "f8"),
            SharedFile(source, "f9"),
            SharedFile(source, "f10"),
            SharedFile(source, "f11"),
            SharedFile(source, "f12"),
            SharedFile(source, "f13"),
            SharedFile(source, "f14"),
            SharedFile(source, "f15"),
            SharedFile(source, "f16"),
            SharedFile(source, "f17"),
            SharedFile(source, "f18")
          )

          for {
            l <- IO.parSequenceN(4)(fs)
          } yield Output(
            l(0),
            l(1),
            l(2),
            Intermediate(l(3)),
            List(Intermediate(l(4))),
            List(List(Intermediate(l(5)))),
            OtherCollection(Intermediate(l(6))),
            OtherCollection(Intermediate(l(16))),
            OtherCollection(Intermediate(l(15))),
            IntermediateMutable(l(7), Some(l(8))),
            Seq(IntermediateMutable(l(9), Some(l(10)))),
            Some(l(11)),
            Some(Intermediate(l(12))),
            IntermediateMutable(l(13), Some(l(14))),
            Map("1" -> Intermediate(l(17)))
          )
    }

  def run = {

    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    println(tmp)
    val testConfig2 =
      ConfigFactory.parseString(
        s"""tasks.fileservice.connectToProxy = true
        
        tasks.fileservice.storageURI=${tmp.getAbsolutePath}
        tasks.fileservice.proxyStorage=true
      hosts.numCPU=0
      tasks.disableRemoting = false
      tasks.elastic.engine = "tasks.elastic.sh.SHElasticSupport"
      tasks.elastic.queueCheckInterval = 3 seconds  
      tasks.addShutdownHook = false
      tasks.failuredetector.acceptable-heartbeat-pause = 5 s
      tasks.worker-main-class = "tasks.TestWorker"
      tasks.elastic.sh.workdir = ${tmp.getAbsolutePath}
      tasks.elastic.javaCommandLine = "-Dtasks.fileservice.connectToProxy=true"
      """
      )

    withTaskSystem(testConfig2.withFallback(testConfig)) { implicit ts =>
      val tmpfile =  java.io.File.createTempFile("dsfsdf","dfs")
        tasks.util.writeBinaryToFile(tmpfile.getAbsolutePath,Array[Byte](1, 2, 3))
      val sf = await(SharedFile(tmpfile, "input.txt"))
      val f1 = testTask(Input(1) -> sf)(ResourceRequest(1, 500))
      val f2 = testTask(Input(1) -> sf)(ResourceRequest(1, 500))
      def getFiles(o: Output) = {
        val untyped = UntypedResult.make(o)
        (untyped.files.toSeq.map(_.file) ++ untyped.mutableFiles.toSeq
          .flatMap(_.toSeq)
          .map(_.file)).map(_.allocated.map(_._1))
      }
      val future = for {
        t1 <- f1
        t1Files <- IO.parSequenceN(4)(getFiles(t1))
        t2 <- f2
        t2Files <- IO.parSequenceN(4)(getFiles(t2))
      } yield (
        t1Files,
        t2Files,
        t1.mutableFiles.map(_.name),
        t1.immutableFiles.map(_.name)
      )

      (future)

    }
  }

}

class SHWithSharedFilesTestSuite extends FunSuite with Matchers {

  test("task output <: ResultWithSharedFiles should be cached ") {
    val (t1Files, t2Files, t1MutablesFiles, t1ImmutablesFiles) =
      SHResultWithSharedFilesTest.run.unsafeRunSync().get
    t1Files.distinct.size shouldBe 18
    (t1Files zip t2Files) foreach { case (f1, f2) =>
      f1 shouldBe f2
      f1.length shouldBe f2.length
      f1.length > 0 shouldBe true
    }
    t1MutablesFiles.sorted shouldBe Seq("f11", "f14", "f15", "f16", "f3", "f9")
    t1ImmutablesFiles.sorted shouldBe Seq(
      "f1",
      "f10",
      "f12",
      "f13",
      "f17",
      "f18",
      "f2",
      "f4",
      "f5",
      "f6",
      "f7",
      "f8"
    )

  }

}
