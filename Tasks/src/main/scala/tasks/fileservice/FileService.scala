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

import com.google.common.hash._

import scala.concurrent.duration._
import scala.concurrent._
import scala.util._

import java.lang.Class
import java.io.{File, InputStream, FileInputStream, BufferedInputStream}
import java.util.concurrent.{TimeUnit, ScheduledFuture}
import java.nio.channels.{WritableByteChannel, ReadableByteChannel}
import java.net.URL

import tasks.util._
import tasks.util.eq._
import tasks.caching._

@SerialVersionUID(1L)
case class FileServiceActor(actor: ActorRef)

@SerialVersionUID(1L)
case class FileServicePrefix(list: Vector[String]) {
  def append(n: String) = FileServicePrefix(list :+ n)
  def propose(name: String) = ProposedManagedFilePath(list :+ name)
}

@SerialVersionUID(1L)
case class ProposedManagedFilePath(list: Vector[String]) {
  def name = list.last
  def toManaged = ManagedFilePath(list)
}

object SharedFileHelper {

  // def getByNameUnchecked(path:FilePath)(implicit service: FileServiceActor, context: ActorRefFactory): Option[SharedFile] = {
  //   Try(SharedFile.getPathToFile(new SharedFile(path, 0, 0), false)).toOption.map { f =>
  //     SharedFile.create(prefix, name, f)
  //   }

  // }

  private[tasks] def createForTesting(name: String) =
    new SharedFile(ManagedFilePath(Vector(name)), 0, 0)
  private[tasks] def createForTesting(name: String, size: Long, hash: Int) =
    new SharedFile(ManagedFilePath(Vector(name)), size, hash)

  private[tasks] def create(size: Long,
                            hash: Int,
                            path: ManagedFilePath): SharedFile =
    new SharedFile(path, size, hash)

  private val isLocal = (f: File) => f.canRead

  def getStreamToFile(sf: SharedFile)(
      implicit service: FileServiceActor,
      context: ActorRefFactory): InputStream = {

    val serviceactor = service.actor
    implicit val timout = akka.util.Timeout(1441 minutes)
    val ac = context.actorOf(
        Props(new FileUserStream(sf, serviceactor, isLocal))
          .withDispatcher("fileuser-dispatcher"))

    val f = Await.result(
        (ac ? WaitingForPath).asInstanceOf[Future[Try[InputStream]]],
        atMost = 1440 minutes)
    ac ! PoisonPill
    f match {
      case Success(r) => r
      case Failure(e) =>
        throw new RuntimeException("getStreamToFile failed. " + sf, e)
    }
  }

  def openStreamToFile[R](sf: SharedFile)(fun: InputStream => R)(
      implicit service: FileServiceActor,
      context: ActorRefFactory): R = useResource(getStreamToFile(sf))(fun)

  def getPathToFile(path: SharedFile)(implicit service: FileServiceActor,
                                      context: ActorRefFactory): File = {

    val serviceactor = service.actor
    implicit val timout = akka.util.Timeout(1441 minutes)
    val ac = context.actorOf(
        Props(new FileUser(path, serviceactor, isLocal))
          .withDispatcher("fileuser-dispatcher"))

    val f = Await.result((ac ? WaitingForPath).asInstanceOf[Future[Try[File]]],
                         atMost = 1440 minutes)
    ac ! PoisonPill
    f match {
      case Success(r) => r
      case Failure(e) =>
        throw new RuntimeException("getPathToFile failed. " + path, e)
    }
  }

  def isAccessible(sf: SharedFile)(implicit service: FileServiceActor,
                                   context: ActorRefFactory): Boolean = {
    implicit val timout = akka.util.Timeout(1441 minutes)
    Await.result(
        (service.actor ? IsAccessible(sf)).asInstanceOf[Future[Boolean]],
        atMost = duration.Duration.Inf)
  }

  def getURL(sf: SharedFile)(implicit service: FileServiceActor,
                             context: ActorRefFactory): URL = {
    sf.path match {
      case x: RemoteFilePath => x.url
      case x: ManagedFilePath => {
        implicit val timout = akka.util.Timeout(1441 minutes)
        Await.result((service.actor ? GetURL(sf)).asInstanceOf[Future[URL]],
                     atMost = duration.Duration.Inf)
      }
    }

  }

}

class FileUserStream(sf: SharedFile,
                     service: ActorRef,
                     isLocal: java.io.File => Boolean)
    extends AbstractFileUser[InputStream](sf, service, isLocal) {

  private var writeableChannel: Option[WritableByteChannel] = None

  def transfertome {
    log.debug("Unreadable")
    val pipe = java.nio.channels.Pipe.open
    writeableChannel = Some(pipe.sink)
    val transferinActor = context.actorOf(
        Props(new TransferIn(writeableChannel.get, self))
          .withDispatcher("transferin"))

    service ! TransferFileToUser(transferinActor, sf)

    result = Some(
        Success(java.nio.channels.Channels.newInputStream(pipe.source)))
    finish
  }

  def finish {
    if (listener.isDefined) {
      listener.get ! result.get
      self ! PoisonPill
    }
  }

  def finishLocalFile(f: File) {
    log.debug("Readable")
    result = Some(Success(new BufferedInputStream(new FileInputStream(f))))
    finish
  }

  def handleCentralStorage(storage: FileStorage) {
    val stream = storage.openStream(sf)
    if (stream.isSuccess) {
      result = Some(Success(stream.get))
      finish
    } else {
      log.debug(
          s"storage.openStream, (KnownPathsWithStorage($storage)): ${stream.toString}, $sf")
      transfertome
    }
  }

  override def receive = super.receive orElse {
    case FileSaved => {
      writeableChannel.get.close
    }
  }
}

class FileUser(sf: SharedFile,
               service: ActorRef,
               isLocal: java.io.File => Boolean)
    extends AbstractFileUser[File](sf, service, isLocal) {

  private var fileUnderTransfer: Option[File] = None
  private var writeableChannel: Option[WritableByteChannel] = None

  def transfertome {
    log.debug("Unreadable")
    val fileToSave = TempFile.createFileInTempFolderIfPossibleWithName(sf.name)
    fileUnderTransfer = Some(fileToSave)
    writeableChannel = Some(
        new java.io.FileOutputStream(fileToSave).getChannel)
    val transferinActor = context.actorOf(
        Props(new TransferIn(writeableChannel.get, self))
          .withDispatcher("transferin"))

    service ! TransferFileToUser(transferinActor, sf)
  }

  def finishLocalFile(f: File) {
    log.debug("Readable")
    result = Some(Success(f))
    if (listener.isDefined) {
      listener.get ! result.get
      self ! PoisonPill
    }
  }

  def handleCentralStorage(storage: FileStorage) {
    val f = storage.exportFile(sf)
    if (f.isSuccess && isLocal(f.get)) {
      service ! NewPath(sf, f.get)
      finishLocalFile(f.get)
    } else {
      log.debug(
          s"storage.export, (KnownPathsWithStorage($storage)): ${f.toString}, $sf")
      transfertome
    }
  }

  override def receive = super.receive orElse {
    case FileSaved => {
      writeableChannel.get.close
      service ! NewPath(sf, fileUnderTransfer.get)
      finishLocalFile(fileUnderTransfer.get)
    }
  }
}

abstract class AbstractFileUser[R](sf: SharedFile,
                                   service: ActorRef,
                                   isLocal: File => Boolean)
    extends Actor
    with akka.actor.ActorLogging {

  var listener: Option[ActorRef] = None
  var result: Option[Try[R]] = None
  var fileNotFound = false

  override def preStart {
    service ! GetPaths(sf)
  }

  protected def transfertome: Unit
  protected def finishLocalFile(file: File): Unit
  protected def handleCentralStorage(storage: FileStorage): Unit

  private def fail(e: Throwable) {
    fileNotFound = true
    if (listener.isDefined) {
      listener.get ! Failure(e)
      self ! PoisonPill
    }
  }

  def receive = {
    case WaitingForPath => {
      listener = Some(sender)
      log.debug("listener:" + listener)
      if (result.isDefined || fileNotFound) {
        sender ! result.get
        self ! PoisonPill
      }
    }
    case FileNotFound(e) => {
      log.warning("NotFound : " + sf + ". Reason: " + e.toString)
      fail(e)
    }
    case CannotSaveFile(e) => {
      log.error("CannotSaveFile : " + sf + " Reason: " + e)
      fail(e)
    }
    case KnownPathsWithStorage(list, storage) => {
      log.debug("KnownPathsWithStorage")
      list.find(isLocal) match {
        case Some(file) => finishLocalFile(file)
        case None => {
          handleCentralStorage(storage)
        }
      }
    }
    case KnownPaths(list) => {
      log.debug("KnownPaths:" + list)
      list.find(isLocal) match {
        case Some(file) => finishLocalFile(file)
        case None => transfertome
      }
    }
    case TryToDownload(storage) => {
      log.debug("trytodownload")
      handleCentralStorage(storage)
    }

  }

}

class FileSender(file: File,
                 proposedPath: ProposedManagedFilePath,
                 service: ActorRef)
    extends Actor
    with akka.actor.ActorLogging {

  override def preStart {
    service ! NewFile(file, proposedPath)
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
      val readablechannel = new java.io.FileInputStream(file).getChannel
      val chunksize = tasks.util.config.global.fileSendChunkSize
      context.actorOf(
          Props(new TransferOut(readablechannel, transferin, chunksize))
            .withDispatcher("transferout"))

    case TryToUpload(storage) =>
      log.debug("trytoupload")
      val f = storage.importFile(file, proposedPath)
      if (f.isSuccess) {
        log.debug("uploaded")
        service ! Uploaded(f.get._1, f.get._2, f.get._3, f.get._4)
      } else {
        log.debug("could not upload. send could not upload and transfer")
        service ! CouldNotUpload(proposedPath)
      }

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

object FileStorage {
  def getContentHash(is: InputStream): Int = {
    val checkedSize = 1024 * 256
    val buffer = Array.fill[Byte](checkedSize)(0)
    val his = new HashingInputStream(Hashing.crc32c, is)

    com.google.common.io.ByteStreams.read(his, buffer, 0, buffer.size)
    his.hash.asInt
  }
}

trait FileStorage extends Serializable {

  def url(sf: SharedFile): URL = sf.path match {
    case x: ManagedFilePath => url(x)
    case x: RemoteFilePath => url(x)
  }

  def url(mp: RemoteFilePath): URL = mp.url

  def url(mp: ManagedFilePath): URL

  def contains(sf: SharedFile): Boolean = sf.path match {
    case x: ManagedFilePath => contains(x, sf.byteSize, sf.hash)
    case x: RemoteFilePath => contains(x, sf.byteSize, sf.hash)
  }

  def contains(path: ManagedFilePath, size: Long, hash: Int): Boolean

  def contains(path: RemoteFilePath, size: Long, hash: Int): Boolean =
    getSizeAndHash(path).map {
      case (size1, hash1) =>
        size1 === size && (tasks.util.config.global.skipContentHashVerificationAfterCache || hash === hash1)
    }.getOrElse(false)

  def importFile(
      f: File,
      path: ProposedManagedFilePath): Try[(Long, Int, File, ManagedFilePath)]

  def getSizeAndHash(path: RemoteFilePath): Try[(Long, Int)] = Try {
    val connection = path.url.openConnection
    val is = connection.getInputStream
    val size1 = connection.getContentLength.toLong
    val hash1 = FileStorage.getContentHash(is)
    is.close
    (size1.toLong, hash1)
  }

  def exportFile(sf: SharedFile): Try[File] =
    if (contains(sf)) {
      sf.path match {
        case x: ManagedFilePath => exportFile(x)
        case x: RemoteFilePath => exportFile(x)
      }
    } else
      Failure(throw new RuntimeException(this + " does not contain " + sf))

  def exportFile(path: ManagedFilePath): Try[File]

  def exportFile(path: RemoteFilePath): Try[File] = {
    openStream(path).map { is =>
      val tmp = TempFile.createTempFile("")
      openFileOutputStream(tmp) { os =>
        com.google.common.io.ByteStreams.copy(is, os)
      }
      tmp
    }
  }

  def openStream(sf: SharedFile): Try[InputStream] =
    if (contains(sf)) {
      sf.path match {
        case x: ManagedFilePath => openStream(x)
        case x: RemoteFilePath => openStream(x)
      }
    } else
      Failure(throw new RuntimeException(this + " does not contain " + sf))

  def openStream(path: ManagedFilePath): Try[InputStream]

  def openStream(path: RemoteFilePath): Try[InputStream] = Try {
    val s = path.url.openStream
    s.read
    s.close
    path.url.openStream
  }

  def centralized: Boolean

  def list(regexp: String): List[SharedFile]

}

class FileService(storage: FileStorage,
                  threadpoolsize: Int = 8,
                  isLocal: File => Boolean = _.canRead)
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

  private val knownPaths =
    collection.mutable.AnyRefMap[SharedFile, List[File]]()

  // transferinactor -> (name,channel,fileinbase,filesender)
  private val transferinactors = collection.mutable
    .Map[ActorRef,
         (WritableByteChannel, File, ActorRef, ProposedManagedFilePath)]()

  private def create(length: Long,
                     hash: Int,
                     path: ManagedFilePath): Future[SharedFile] = {
    Future {
      ((SharedFileHelper.create(length, hash, path)))
    }(ec)
  }

  private def createRemote(path: RemoteFilePath): Future[Try[SharedFile]] = {
    Future {
      storage.getSizeAndHash(path).map {
        case (size, hash) =>
          new SharedFile(path, size, hash)
      }
    }(ec)
  }

  private def recordToNames(file: File, sf: SharedFile): Unit = {
    if (knownPaths.contains(sf)) {
      val oldlist = knownPaths(sf)
      knownPaths.update(sf, (file.getCanonicalFile :: oldlist).distinct)
    } else {
      knownPaths.update(sf, List(file))
    }

  }

  def receive = {
    case NewRemote(url) =>
      try {
        val remotepath = RemoteFilePath(url)
        val trySf = createRemote(remotepath)
        trySf.failed.foreach(e => log.error(e, "Error {}", url))
        trySf.pipeTo(sender)
      } catch {
        case e: Exception => {
          log.error(e, "Error while accessing storage " + url)
          sender ! Failure(e)
        }
      }
    case NewFile(file, proposedPath) =>
      try {
        if (isLocal(file)) {

          val (length, hash, f, managedFilePath) =
            storage.importFile(file, proposedPath).get

          val sn = create(length, hash, managedFilePath)
          sn.foreach(sf => recordToNames(f, sf))

          sn.failed.foreach { e =>
            log.error(e,
                      "Error in creation of SharedFile {} {}",
                      file,
                      proposedPath)
          }

          sn.pipeTo(sender)

        } else {

          if (storage.centralized) {
            log.debug("answer trytoupload")
            sender ! TryToUpload(storage)

          } else {
            // transfer
            val savePath = TempFile.createFileInTempFolderIfPossibleWithName(
                proposedPath.name)
            val writeableChannel =
              new java.io.FileOutputStream(savePath).getChannel
            val transferinActor = context.actorOf(
                Props(new TransferIn(writeableChannel, self))
                  .withDispatcher("transferin"))
            transferinactors.update(
                transferinActor,
                (writeableChannel, savePath, sender, proposedPath))

            sender ! TransferToMe(transferinActor)
          }

        }
      } catch {
        case e: Exception => {
          log.error(
              e,
              "Error while accessing storage " + file + " " + proposedPath)
          sender ! ErrorWhileAccessingStore
        }
      }
    case Uploaded(length, hash, file, managedFilePath) => {
      log.debug("got uploaded. record")

      val sn = create(length, hash, managedFilePath)
      sn.foreach(sf => recordToNames(file, sf))
      sn.failed.foreach { e =>
        log.error(e,
                  "Error in creation of SharedFile {} {}",
                  file,
                  managedFilePath)
      }
      sn pipeTo sender
    }
    case CannotSaveFile(e) => {
      transferinactors.get(sender).foreach {
        case (channel, file, filesender, proposedPath) =>
          channel.close
          log.error("CannotSaveFile(" + e + ")")
          filesender ! ErrorWhileAccessingStore(e)
      }
      transferinactors.remove(sender)
    }
    case FileSaved => {
      transferinactors.get(sender).foreach {
        case (channel, file, filesender, proposedPath) =>
          channel.close
          try {
            val (length, hash, f, managedFilePath) =
              storage.importFile(file, proposedPath).get

            val sn = create(length, hash, managedFilePath)

            sn.foreach(sf => recordToNames(file, sf))

            sn.failed.foreach { e =>
              log.error(e,
                        "Error in creation of SharedFile {} {}",
                        file,
                        proposedPath)
            }

            sn pipeTo filesender

          } catch {
            case e: Exception => {
              log.error(e, "Error while accessing storage");
              filesender ! ErrorWhileAccessingStore
            }
          }
      }
      transferinactors.remove(sender)
    }
    case CouldNotUpload(proposedPath) => {
      val savePath =
        TempFile.createFileInTempFolderIfPossibleWithName(proposedPath.name)
      val writeableChannel = new java.io.FileOutputStream(savePath).getChannel
      val transferinActor = context.actorOf(
          Props(new TransferIn(writeableChannel, self))
            .withDispatcher("transferin"))
      transferinactors.update(
          transferinActor,
          (writeableChannel, savePath, sender, proposedPath))

      sender ! TransferToMe(transferinActor)

    }
    case GetPaths(sf) =>
      try {
        knownPaths.get(sf) match {
          case Some(l) => {
            if (storage.centralized) {
              sender ! KnownPathsWithStorage(l, storage)
            } else {
              sender ! KnownPaths(l)
            }

          }
          case None => {
            if (storage.contains(sf)) {
              if (storage.centralized) {
                sender ! TryToDownload(storage)
              } else {
                val f = storage.exportFile(sf).get
                recordToNames(f, sf)
                sender ! KnownPaths(List(f))
              }
            } else {
              sender ! FileNotFound(
                  new RuntimeException(
                      s"SharedFile not found in storage. $storage # contains($sf) returned false. " + sf))
            }
          }
        }
      } catch {
        case e: Exception => {
          log.error(e.toString)
          sender ! FileNotFound(e)
        }
      }
    case TransferFileToUser(transferinActor, sf) =>
      try {
        val file = knownPaths(sf).find(isLocal) match {
          case Some(x) => x
          case None => storage.exportFile(sf).get
        }
        val readablechannel = new java.io.FileInputStream(file).getChannel
        val chunksize = tasks.util.config.global.fileSendChunkSize
        context.actorOf(
            Props(new TransferOut(readablechannel, transferinActor, chunksize))
              .withDispatcher("transferout"))

      } catch {
        case e: Exception => {
          log.error(e.toString)
          sender ! FileNotFound(e)
        }
      }
    case NewPath(sf, filePath) => {

      recordToNames(filePath, sf)

    }
    case GetListOfFilesInStorage(regexp) => sender ! storage.list(regexp)
    case IsAccessible(sf) => sender ! storage.contains(sf)
    case GetURL(sf) => sender ! storage.url(sf)
  }
}
