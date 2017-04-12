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

package tasks.fileservice

import akka.actor._
import akka.pattern.ask
import akka.pattern.pipe
import akka.stream.scaladsl._
import akka.stream._
import akka.util._

import com.google.common.hash._

import scala.concurrent.duration._
import scala.concurrent._
import scala.util._

import java.lang.Class
import java.io.{File, InputStream, FileInputStream, BufferedInputStream}
import java.util.concurrent.{TimeUnit, ScheduledFuture}
import java.nio.channels.{WritableByteChannel, ReadableByteChannel}

import tasks.util._
import tasks.util.eq._
import tasks.caching._
import tasks.queue._

class FileSender(file: File,
                 proposedPath: ProposedManagedFilePath,
                 deleteLocalFile: Boolean,
                 service: ActorRef)
    extends Actor
    with akka.actor.ActorLogging {

  override def preStart {
    service ! NewFile(file, proposedPath, ephemeralFile = deleteLocalFile)
  }

  var sharedFile: Option[SharedFile] = None
  var error = false
  var listener: Option[ActorRef] = None

  def receive = {
    case t: SharedFile =>
      sharedFile = Some(t)
      if (deleteLocalFile) {
        file.delete
      }
      if (listener.isDefined) {
        listener.get ! sharedFile
        self ! PoisonPill
      }

    case TransferToMe(transferin) =>
      val readablechannel = new java.io.FileInputStream(file).getChannel
      val chunksize = tasks.util.config.global.fileSendChunkSize
      context.actorOf(
          Props(new TransferOut(readablechannel, transferin, chunksize))
            .withDispatcher("transferout"))

    case WaitingForSharedFile =>
      listener = Some(sender)
      if (sharedFile.isDefined) {
        sender ! sharedFile
        self ! PoisonPill
      } else if (error) {
        sender ! None
        self ! PoisonPill
      }

    case ErrorWhileAccessingStore(e) =>
      error = true
      log.error("ErrorWhileAccessingStore: " + e)
      listener.foreach { x =>
        x ! None
        self ! PoisonPill
      }

  }

}

class SourceSender(file: Source[ByteString, _],
                   proposedPath: ProposedManagedFilePath,
                   service: ActorRef)
    extends Actor
    with akka.actor.ActorLogging {

  implicit val mat = ActorMaterializer()

  override def preStart {
    service ! NewSource(proposedPath)
  }

  var sharedFile: Option[SharedFile] = None
  var error = false
  var listener: Option[ActorRef] = None

  def receive = {
    case t: SharedFile =>
      sharedFile = Some(t)
      if (listener.isDefined) {
        listener.get ! sharedFile
        self ! PoisonPill
      }

    case TransferToMe(transferin) =>
      val is = file.runWith(StreamConverters.asInputStream())
      val readablechannel = java.nio.channels.Channels.newChannel(is)
      val chunksize = tasks.util.config.global.fileSendChunkSize
      context.actorOf(
          Props(new TransferOut(readablechannel, transferin, chunksize))
            .withDispatcher("transferout"))

    case WaitingForSharedFile =>
      listener = Some(sender)
      if (sharedFile.isDefined) {
        sender ! sharedFile
        self ! PoisonPill
      } else if (error) {
        sender ! None
        self ! PoisonPill
      }

    case ErrorWhileAccessingStore(e) =>
      error = true
      log.error("ErrorWhileAccessingStore: " + e)
      listener.foreach { x =>
        x ! None
        self ! PoisonPill
      }
  }

}
