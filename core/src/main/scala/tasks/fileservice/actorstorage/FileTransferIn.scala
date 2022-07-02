/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
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

package tasks.fileservice.actorfilestorage

import akka.actor.{Actor, ActorRef, PoisonPill}
import java.nio.channels.{WritableByteChannel, ReadableByteChannel}
import java.nio.ByteBuffer
import scala.util.{Try, Failure, Success}
import tasks.wire.filetransfermessages._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import scala.concurrent.duration._

class TransferIn(output: WritableByteChannel, notification: ActorRef)
    extends Actor
    with akka.actor.ActorLogging {

  def receive = {
    case Chunk(bytestring, _) =>
      Try {
        output.write(bytestring.asReadOnlyByteBuffer)
      } match {
        case Success(_) =>
          sender() ! Ack()
        case Failure(e) => {
          notification ! CannotSaveFile(e.getMessage)
          sender() ! CannotSaveFile(e.getMessage)
          self ! PoisonPill
        }
      }
    case CannotSaveFile(ex, _) =>
      notification ! CannotSaveFile(ex)
      self ! PoisonPill
    case EndChunk(_) =>
      notification ! FileSaved()
      self ! PoisonPill

  }
}

class TransferOut(
    file: ReadableByteChannel,
    transferIn: ActorRef,
    chunkSize: Int,
    release: IO[Unit]
) extends Actor
    with akka.actor.ActorLogging {

  val buffer: ByteBuffer = ByteBuffer.allocate(chunkSize)

  var eof = false

  private def readAhead(): Unit = {
    val count = file.read(buffer)
    buffer.position(0)

    if (count == -1) {
      eof = true
      file.close
    } else {
      buffer.limit(count)
    }
  }

  private def send(): Unit = {
    readAhead()

    if (eof) {
      transferIn ! EndChunk()
      self ! PoisonPill
      release.timeout(120 seconds).unsafeRunSync()
    } else {
      transferIn ! Chunk(com.google.protobuf.ByteString.copyFrom(buffer))
    }
    buffer.rewind

  }

  override def preStart(): Unit = {
    log.debug("FileTransferOut start")
    send()
  }

  def receive = {
    case Ack(_) => send()
    case CannotSaveFile(_, _) =>
      self ! PoisonPill
      release.timeout(120 seconds).unsafeRunSync()
  }
}
