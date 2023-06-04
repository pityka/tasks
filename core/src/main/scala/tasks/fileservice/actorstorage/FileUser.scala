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

package tasks.fileservice.actorfilestorage

import tasks.fileservice._

import akka.actor._
import akka.stream.scaladsl._
import akka.util._

import scala.util._

import java.io.File
import java.nio.channels.WritableByteChannel

import tasks.util._
import tasks.wire._

class FileUserSource(
    sf: ManagedFilePath,
    service: ActorRef,
    fromOffset: Long
) extends AbstractFileUser[Source[ByteString, _]](sf, 0, 0, service) {

  private var writeableChannel: Option[WritableByteChannel] = None

  override def preStart(): Unit = {
    self ! AckFileIsPresent
  }

  def transfertome(): Unit = {
    log.debug("Unreadable")
    val pipe = java.nio.channels.Pipe.open
    writeableChannel = Some(pipe.sink)
    val transferinActor = context.actorOf(
      Props(new TransferIn(writeableChannel.get, self))
        .withDispatcher("transferin")
    )

    service ! TransferFileToUser(transferinActor, sf, fromOffset)

    result = Some(
      Success(
        StreamConverters.fromInputStream(() =>
          java.nio.channels.Channels.newInputStream(pipe.source)
        )
      )
    )
    finish()
  }

  def finish(): Unit = {
    if (listener.isDefined) {
      listener.get ! result.get
    }
  }


  override def receive = super.receive orElse {
    case filetransfermessages.FileSaved(_) => {
      writeableChannel.get.close
      self ! PoisonPill
    }
  }
}

class FileUser(
    sf: ManagedFilePath,
    size: Long,
    hash: Int,
    service: ActorRef,
) extends AbstractFileUser[File](sf, size, hash, service) {

  private var fileUnderTransfer: Option[File] = None
  private var writeableChannel: Option[WritableByteChannel] = None

  def transfertome() = {
    log.debug("Unreadable")
    val fileToSave = TempFile.createFileInTempFolderIfPossibleWithName(sf.name)
    fileUnderTransfer = Some(fileToSave)
    writeableChannel = Some(new java.io.FileOutputStream(fileToSave).getChannel)
    val transferinActor = context.actorOf(
      Props(new TransferIn(writeableChannel.get, self))
        .withDispatcher("transferin")
    )

    service ! TransferFileToUser(transferinActor, sf, 0L)
  }

  def finishLocalFile(f: File) = {
    log.debug("Readable")
    result = Some(Success(f))
    if (listener.isDefined) {
      listener.get ! result.get
      self ! PoisonPill
    }
  }

  override def receive = super.receive orElse {
    case filetransfermessages.FileSaved(_) => {
      writeableChannel.get.close
      finishLocalFile(fileUnderTransfer.get)
    }
  }
}

abstract class AbstractFileUser[R](
    sf: ManagedFilePath,
    size: Long,
    hash: Int,
    service: ActorRef,
) extends Actor
    with akka.actor.ActorLogging {

  var listener: Option[ActorRef] = None
  var result: Option[Try[R]] = None
  var fileNotFound = false

  override def preStart() = {
    service ! AskForFile(sf, size, hash)
  }

  protected def transfertome(): Unit

  private def fail(e: Throwable) = {
    fileNotFound = true
    if (listener.isDefined) {
      listener.get ! Failure(e)
      self ! PoisonPill
    }
  }

  def receive = {
    case WaitingForPath => {
      listener = Some(sender())
      log.debug("listener:" + listener)
      if (result.isDefined || fileNotFound) {
        sender() ! result.getOrElse(Failure(new RuntimeException("not found")))
        self ! PoisonPill
      }
    }
    case FileNotFound(e) => {
      if (size >= 0) {
        log.warning("NotFound : " + sf + ". Reason: " + e.toString)
      }
      fail(e)
    }
    case filetransfermessages.CannotSaveFile(e, _) => {
      log.error("CannotSaveFile : " + sf + " Reason: " + e)
      fail(new RuntimeException(e))
    }
    case AckFileIsPresent => {
      log.debug(s"AckFileIsPresent $sf")
        transfertome()
    }

  }

}
