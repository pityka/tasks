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

package tasks.util

import akka.actor.{Actor, ActorRef, PoisonPill}
import akka.util.ByteString
import java.nio.channels.{WritableByteChannel, ReadableByteChannel}
import java.nio.ByteBuffer
import java.io.FileInputStream
import scala.util.{Try, Failure, Success}

sealed trait FileTransferMessage extends Serializable
case class Chunk(data: ByteString) extends FileTransferMessage
case object Ack extends FileTransferMessage
case object EndChunk extends FileTransferMessage
case object FileSaved extends FileTransferMessage
case class CannotSaveFile(e: Throwable) extends FileTransferMessage

class TransferIn(output: WritableByteChannel, notification: ActorRef)
    extends Actor
    with akka.actor.ActorLogging {

  def receive = {
    case Chunk(bytestring) =>
      Try {
        output.write(bytestring.asByteBuffer)
      } match {
        case Success(_) => sender ! Ack
        case Failure(e) => {
          notification ! CannotSaveFile(e)
          sender ! CannotSaveFile(e)
          self ! PoisonPill
        }
      }

    case EndChunk =>
      notification ! FileSaved
      self ! PoisonPill

  }
}

class TransferOut(file: ReadableByteChannel,
                  transferIn: ActorRef,
                  chunkSize: Int)
    extends Actor
    with akka.actor.ActorLogging {

  val buffer: ByteBuffer = ByteBuffer.allocate(chunkSize)

  var eof = false

  private def readAhead {
    val count = file.read(buffer)
    buffer.position(0)

    if (count == -1) {
      eof = true
      file.close
    } else {
      buffer.limit(count)
    }
  }

  private def send {
    readAhead

    if (eof) {
      transferIn ! EndChunk
      self ! PoisonPill
    } else {
      transferIn ! Chunk(ByteString(buffer))
    }
    buffer.rewind

  }

  override def preStart {
    log.debug("FileTransferOut start")
    send
  }

  def receive = {
    case Ack => send
    case CannotSaveFile(_) => self ! PoisonPill
  }
}
