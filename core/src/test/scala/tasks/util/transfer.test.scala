package tasks.util

import org.scalatest._
import org.scalatest.funspec.{AnyFunSpecLike => FunSpecLike}
import scala.concurrent.duration._
import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import akka.actor.{Props, ActorSystem}
import com.typesafe.config.ConfigFactory

import org.scalatest.matchers.should.Matchers

import tasks.wire.filetransfermessages._

object Conf {
  val str = """my-pinned-dispatcher {
  executor = "thread-pool-executor"
  type = PinnedDispatcher
  thread-pool-executor.allow-core-timeout=off
}
akka.loglevel = "OFF" """
}

class TransferSpec
    extends TestKit(
      ActorSystem(
        "testsystem",
        ConfigFactory
          .parseString(Conf.str)
          .withFallback(ConfigFactory.load("akkaoverrides.conf"))
      )
    )
    with ImplicitSender
    with FunSpecLike
    with Matchers
    with BeforeAndAfterAll {
  self: Suite =>

  override def afterAll() = {
    Thread.sleep(1500)
    system.terminate()

  }

  describe("transfer files ") {
    it("simple by 1") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val chunksize = 1
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input.getCanonicalPath, data)
      val output = TempFile.createTempFile(".out")

      val writeablechannel = new java.io.FileOutputStream(output).getChannel
      val readablechannel = new java.io.FileInputStream(input).getChannel

      val transferin =
        system.actorOf(Props(new TransferIn(writeablechannel, testActor)))
      system.actorOf(
        Props(new TransferOut(readablechannel, transferin, chunksize))
      )

      expectMsg(1000 millis, FileSaved())

      readBinaryFile(output.getCanonicalPath).toVector should equal(
        data.toVector
      )

    }

    it("simple by 5") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val chunksize = 5
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input.getCanonicalPath, data)
      val output = TempFile.createTempFile(".out")

      val writeablechannel = new java.io.FileOutputStream(output).getChannel
      val readablechannel = new java.io.FileInputStream(input).getChannel

      val transferin =
        system.actorOf(Props(new TransferIn(writeablechannel, testActor)))
      system.actorOf(
        Props(new TransferOut(readablechannel, transferin, chunksize))
      )

      expectMsg(100 millis, FileSaved())

      readBinaryFile(output.getCanonicalPath).toVector should equal(
        data.toVector
      )

    }

    it("simple by 16") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val chunksize = 16
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input.getCanonicalPath, data)
      val output = TempFile.createTempFile(".out")

      val writeablechannel = new java.io.FileOutputStream(output).getChannel
      val readablechannel = new java.io.FileInputStream(input).getChannel

      val transferin =
        system.actorOf(Props(new TransferIn(writeablechannel, testActor)))
      system.actorOf(
        Props(new TransferOut(readablechannel, transferin, chunksize))
      )

      expectMsg(100 millis, FileSaved())

      readBinaryFile(output.getCanonicalPath).toVector should equal(
        data.toVector
      )

    }

    it("simple by 50") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val chunksize = 50
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input.getCanonicalPath, data)
      val output = TempFile.createTempFile(".out")

      val writeablechannel = new java.io.FileOutputStream(output).getChannel
      val readablechannel = new java.io.FileInputStream(input).getChannel

      val transferin =
        system.actorOf(Props(new TransferIn(writeablechannel, testActor)))
      system.actorOf(
        Props(new TransferOut(readablechannel, transferin, chunksize))
      )

      expectMsg(100 millis, FileSaved())

      readBinaryFile(output.getCanonicalPath).toVector should equal(
        data.toVector
      )

    }

    it("empty") {
      val data = Array[Byte]()
      val chunksize = 16
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input.getCanonicalPath, data)
      val output = TempFile.createTempFile(".out")

      val writeablechannel = new java.io.FileOutputStream(output).getChannel
      val readablechannel = new java.io.FileInputStream(input).getChannel

      val transferin =
        system.actorOf(Props(new TransferIn(writeablechannel, testActor)))
      system.actorOf(
        Props(new TransferOut(readablechannel, transferin, chunksize))
      )

      expectMsg(100 millis, FileSaved())

      readBinaryFile(output.getCanonicalPath).toVector should equal(
        data.toVector
      )

    }

    it("simple by 5 stream") {
      val data = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7)
      val chunksize = 5
      val input = TempFile.createTempFile(".in")
      writeBinaryToFile(input.getCanonicalPath, data)

      val pipe = java.nio.channels.Pipe.open

      val writeablechannel = pipe.sink
      val readablechannel = new java.io.FileInputStream(input).getChannel

      val transferin =
        system.actorOf(Props(new TransferIn(writeablechannel, testActor)))
      system.actorOf(
        Props(new TransferOut(readablechannel, transferin, chunksize))
      )

      expectMsg(100 millis, FileSaved())
      writeablechannel.close

      readBinaryStream(
        java.nio.channels.Channels.newInputStream(pipe.source)
      ).toVector should equal(
        data.toVector
      )

    }
  }

}
