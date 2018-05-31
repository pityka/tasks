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
import akka.pattern.pipe
import scala.concurrent._

import java.io.File
import java.nio.channels.{WritableByteChannel}

import tasks.util._
import tasks.util.config._
import tasks.wire._

import io.circe.generic.semiauto._
import io.circe.{Encoder, Decoder}

case class FileServiceActor(actor: ActorRef,
                            storage: Option[ManagedFileStorage],
                            remote: RemoteFileStorage)

case class FileServicePrefix(list: Vector[String], history: Option[History]) {
  def append(n: String) = FileServicePrefix(list :+ n, history)
  def propose(name: String) = ProposedManagedFilePath(list :+ name, history)
  def withHistory(history: History) = copy(history = Some(history))
}
object FileServicePrefix {
  implicit val encoder: Encoder[FileServicePrefix] =
    deriveEncoder[FileServicePrefix]
  implicit val decoder: Decoder[FileServicePrefix] =
    deriveDecoder[FileServicePrefix]
}

case class ProposedManagedFilePath(list: Vector[String],
                                   history: Option[History]) {
  def name = list.last
  def toManaged = ManagedFilePath(list)
}

object ProposedManagedFilePath {
  implicit val enc: Encoder[ProposedManagedFilePath] =
    deriveEncoder[ProposedManagedFilePath]
  implicit val dec: Decoder[ProposedManagedFilePath] =
    deriveDecoder[ProposedManagedFilePath]
}

class FileService(
    storage: ManagedFileStorage,
    threadpoolsize: Int = 8,
    isLocal: File => Boolean = _.canRead)(implicit config: TasksConfig)
    extends Actor
    with akka.actor.ActorLogging {

  val fjp = tasks.util.concurrent.newJavaForkJoinPoolWithNamePrefix(
    "fileservice-recordtonames",
    threadpoolsize)
  val ec = ExecutionContext.fromExecutorService(fjp)

  import context.dispatcher

  override def postStop {
    fjp.shutdown
    log.info("FileService stopped.")
  }

  override def preStart {
    log.info("FileService will start.")
  }

  // transferinactor -> (name,channel,fileinbase,filesender)
  private val transferinactors =
    collection.mutable.Map[ActorRef,
                           (WritableByteChannel,
                            File,
                            ActorRef,
                            ProposedManagedFilePath,
                            Boolean)]()

  private def create(length: Long,
                     hash: Int,
                     path: ManagedFilePath,
                     history: Option[History]): Future[SharedFile] = {
    Future {
      ((SharedFileHelper.create(length, hash, path, history)))
    }(ec)
  }

  def receive = {
    case NewFile(file, proposedPath, ephemeral) =>
      try {
        if (isLocal(file)) {

          storage
            .importFile(file, proposedPath)
            .flatMap {
              case (length, hash, _, managedFilePath) =>
                create(length, hash, managedFilePath, proposedPath.history)
                  .recover {
                    case e =>
                      log.error(e,
                                "Error in creation of SharedFile {} {}",
                                file,
                                proposedPath)
                      throw e
                  }
            }
            .pipeTo(sender)

        } else {

          val savePath =
            TempFile.createFileInTempFolderIfPossibleWithName(proposedPath.name)
          val writeableChannel =
            new java.io.FileOutputStream(savePath).getChannel
          val transferinActor = context.actorOf(
            Props(new TransferIn(writeableChannel, self))
              .withDispatcher("transferin"))
          transferinactors.update(
            transferinActor,
            (writeableChannel, savePath, sender, proposedPath, ephemeral))

          sender ! TransferToMe(transferinActor)

        }
      } catch {
        case e: Exception => {
          log.error(
            e,
            "Error while accessing storage " + file + " " + proposedPath)
          sender ! ErrorWhileAccessingStore
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
            .withDispatcher("transferin"))
        transferinactors.update(
          transferinActor,
          (writeableChannel, savePath, sender, proposedPath, true))

        sender ! TransferToMe(transferinActor)

      } catch {
        case e: Exception => {
          log.error(e, "Error while accessing storage " + proposedPath)
          sender ! ErrorWhileAccessingStore
        }
      }

    case filetransfermessages.CannotSaveFile(e) => {
      transferinactors.get(sender).foreach {
        case (channel, _, filesender, _, _) =>
          channel.close
          log.error("CannotSaveFile(" + e + ")")
          filesender ! ErrorWhileAccessingStore(new RuntimeException(e))
      }
      transferinactors.remove(sender)
    }
    case filetransfermessages.FileSaved() => {
      transferinactors.get(sender).foreach {
        case (channel, file, filesender, proposedPath, _) =>
          channel.close
          try {
            storage.importFile(file, proposedPath).flatMap {
              case (length, hash, _, managedFilePath) =>
                create(length, hash, managedFilePath, proposedPath.history)
                  .recover {
                    case e =>
                      log.error(e,
                                "Error in creation of SharedFile {} {}",
                                file,
                                proposedPath)
                      throw e
                  }

            } pipeTo filesender

          } catch {
            case e: Exception => {
              log.error(e, "Error while accessing storage");
              filesender ! ErrorWhileAccessingStore
            }
          }
      }
      transferinactors.remove(sender)
    }

    case GetPaths(managedPath, size: Long, hash: Int) =>
      try {
        storage
          .contains(managedPath, size, hash)
          .flatMap { contains =>
            if (contains)
              storage.exportFile(managedPath).map(f => KnownPaths(List(f)))
            else
              Future.successful(FileNotFound(new RuntimeException(
                s"SharedFile not found in storage. $storage # contains($managedPath) returned false. ")))
          }
          .pipeTo(sender)
      } catch {
        case e: Exception => {
          log.error(e.toString)
          sender ! FileNotFound(e)
        }
      }
    case TransferFileToUser(transferinActor, sf) =>
      try {
        storage.exportFile(sf).foreach { file =>
          val readablechannel = new java.io.FileInputStream(file).getChannel
          val chunksize = config.fileSendChunkSize
          context.actorOf(
            Props(new TransferOut(readablechannel, transferinActor, chunksize))
              .withDispatcher("transferout"))
        }

      } catch {
        case e: Exception => {
          log.error(e.toString)
          sender ! FileNotFound(e)
        }
      }

    case GetListOfFilesInStorage(regexp) => sender ! storage.list(regexp)
    case IsAccessible(managedPath, size, hash) =>
      sender ! storage.contains(managedPath, size, hash)
    case GetUri(managedPath) => sender ! storage.uri(managedPath)
  }
}
