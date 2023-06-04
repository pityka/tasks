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
import akka.pattern.pipe
import scala.concurrent._

import java.io.File
import java.nio.channels.{WritableByteChannel}

import tasks.util._
import tasks.wire._
import akka.stream.scaladsl.Sink
import tasks.wire.filetransfermessages.EndChunk
import tasks.wire.filetransfermessages.CannotSaveFile
import akka.util.ByteString
import tasks.wire.filetransfermessages.Chunk
import akka.stream.Materializer

class FileServiceProxy(
    storage: ManagedFileStorage,
    threadpoolsize: Int = 8
)(implicit mat: Materializer)
    extends Actor
    with akka.actor.ActorLogging {

  val fjp = tasks.util.concurrent.newJavaForkJoinPoolWithNamePrefix(
    "fileservice-recordtonames",
    threadpoolsize
  )
  val ec = ExecutionContext.fromExecutorService(fjp)

  import context.dispatcher

  override def postStop() = {
    fjp.shutdown
    log.info("FileService stopped.")
  }

  override def preStart() = {
    log.info("FileService will start.")
  }

  // transferinactor -> (name,channel,fileinbase,filesender)
  private val transferinactors =
    collection.mutable.Map[
      ActorRef,
      (WritableByteChannel, File, ActorRef, ProposedManagedFilePath, Boolean)
    ]()

  private def create(
      length: Long,
      hash: Int,
      path: ManagedFilePath
  ): Future[SharedFile] = {
    Future {
      ((SharedFileHelper.create(length, hash, path)))
    }(ec)
  }

  def receive = {
    case GetSharedFolder(prefix) => sender() ! storage.sharedFolder(prefix)
    case NewFile(file, proposedPath, ephemeral) =>
      try {

        val savePath =
          TempFile.createFileInTempFolderIfPossibleWithName(proposedPath.name)
        val writeableChannel =
          new java.io.FileOutputStream(savePath).getChannel
        val transferinActor = context.actorOf(
          Props(new TransferIn(writeableChannel, self))
            .withDispatcher("transferin")
        )
        transferinactors.update(
          transferinActor,
          (writeableChannel, savePath, sender(), proposedPath, ephemeral)
        )

        sender() ! TransferToMe(transferinActor)

      } catch {
        case e: Exception => {
          log.error(
            e,
            "Error while accessing storage " + file + " " + proposedPath
          )
          sender() ! ErrorWhileAccessingStore
        }
      }

    case NewSource(proposedPath) =>
      try {
        val savePath =
          TempFile.createFileInTempFolderIfPossibleWithName(proposedPath.name)
        val writeableChannel =
          new java.io.FileOutputStream(savePath).getChannel
        val transferinActor = context.actorOf(
          Props(new TransferIn(writeableChannel, self))
            .withDispatcher("transferin")
        )
        transferinactors.update(
          transferinActor,
          (writeableChannel, savePath, sender(), proposedPath, true)
        )

        sender() ! TransferToMe(transferinActor)

      } catch {
        case e: Exception => {
          log.error(e, "Error while accessing storage " + proposedPath)
          sender() ! ErrorWhileAccessingStore
        }
      }

    case filetransfermessages.CannotSaveFile(e, _) => {
      transferinactors.get(sender()).foreach {
        case (channel, _, filesender, _, _) =>
          channel.close
          log.error("CannotSaveFile(" + e + ")")
          filesender ! ErrorWhileAccessingStore(new RuntimeException(e))
      }
      transferinactors.remove(sender())
    }
    case filetransfermessages.FileSaved(_) => {
      transferinactors.get(sender()).foreach {
        case (channel, file, filesender, proposedPath, _) =>
          channel.close
          try {
            storage.importFile(file, proposedPath).flatMap {
              case (length, hash, managedFilePath) =>
                create(length, hash, managedFilePath)
                  .recover { case e =>
                    log.error(
                      e,
                      "Error in creation of SharedFile {} {}",
                      file,
                      proposedPath
                    )
                    throw e
                  }
                  .andThen { case _ =>
                    try { file.delete }
                    catch {
                      case e: Throwable =>
                        log.error(
                          e,
                          "Can't delete temporary file {} {}",
                          file,
                          proposedPath
                        )
                    }
                  }

            } pipeTo filesender

          } catch {
            case e: Exception => {
              log.error(e, "Error while accessing storage");
              filesender ! ErrorWhileAccessingStore
            }
          }
      }
      transferinactors.remove(sender())
    }

    case AskForFile(managedPath, size: Long, hash: Int) =>
      try {
        storage
          .contains(managedPath, size, hash)
          .flatMap { contains =>
            if (contains)
              Future.successful(AckFileIsPresent)
            else
              Future.successful(
                FileNotFound(
                  new RuntimeException(
                    s"SharedFile not found in storage. $storage # contains($managedPath) returned false. "
                  )
                )
              )
          }
          .pipeTo(sender())
      } catch {
        case e: Exception => {
          log.error(e.toString)
          sender() ! FileNotFound(e)
        }
      }
    case TransferFileToUser(transferinActor, sf, fromOffset) =>
      try {
        val relay = context.actorOf(Props(new Actor {
          var sourceActor: ActorRef = null
          def receive: Receive = {
            case "init" =>
              sourceActor = sender()
              sourceActor ! true
            case "complete" =>
              transferinActor ! EndChunk()
              self ! PoisonPill
            case e: Throwable =>
              transferinActor ! CannotSaveFile(e.getMessage)
              self ! PoisonPill
            case bs: ByteString =>
              transferinActor ! Chunk(
                com.google.protobuf.ByteString.copyFrom(bs.toArray)
              )
            case tasks.wire.filetransfermessages.Ack => sourceActor ! true
          }
        }))
        storage
          .createSource(sf, fromOffset)
          .to(
            Sink.actorRefWithBackpressure(
              ref = relay,
              onInitMessage = "init",
              onCompleteMessage = "complete",
              onFailureMessage = e => e
            )
          )
          .run()

      } catch {
        case e: Exception => {
          log.error(e.toString)
          sender() ! FileNotFound(e)
        }
      }

    case IsAccessible(managedPath, size, hash) =>
      storage.contains(managedPath, size, hash).pipeTo(sender())
    case IsPathAccessible(managedPath, retrieveSizeAndHash) =>
      storage.contains(managedPath, retrieveSizeAndHash).pipeTo(sender())
    case GetUri(managedPath) => sender() ! storage.uri(managedPath)
    case Delete(managedPath, size, hash) =>
      storage.delete(managedPath, size, hash).pipeTo(sender())
  }
}
