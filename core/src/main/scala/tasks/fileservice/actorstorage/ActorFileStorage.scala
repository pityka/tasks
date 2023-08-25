package tasks.fileservice.actorfilestorage

import tasks.fileservice._
import akka.actor.ActorRef
import akka.pattern.ask
import scala.concurrent.duration._
import tasks.wire._
import tasks.util.Uri
import akka.stream.scaladsl.Source
import akka.util.ByteString
import akka.actor._
import scala.util._
import scala.concurrent._
import java.io.File
import cats.effect.kernel.Resource
import akka.stream.scaladsl.Sink
import cats.effect.IO
import tasks.util.config.TasksConfig
import akka.stream.Materializer
import tasks.util.SimpleSocketAddress

object ActorFileStorage {
  def startFileServiceActor(storage: ManagedFileStorage)(implicit
      system: ActorRefFactory,
      mat: Materializer
  ): ActorRef = {

    system.actorOf(
      Props(new FileServiceProxy(storage))
        .withDispatcher("fileservice-pinned"),
      "fileservice"
    )

  }

  def connectToRemote(address: SimpleSocketAddress)(implicit
      config: TasksConfig,
      system: ActorRefFactory,
      mat: Materializer,
      ec: ExecutionContext
  ) = {
    val actorPath =
      s"akka://tasks@${address.getHostName}:${address.getPort}/user/fileservice"
    val remoteFileServiceActor = Await.result(
      system.actorSelection(actorPath).resolveOne(600 seconds),
      atMost = 600 seconds
    )

    new ActorFileStorage(remoteFileServiceActor)
  }
}

class ActorFileStorage(
    fileServiceActor: ActorRef
)(implicit
    val ec: ExecutionContext,
    config: TasksConfig,
    mat: Materializer,
    context: ActorRefFactory
) extends ManagedFileStorage {

  def uri(mp: ManagedFilePath): IO[Uri] = {
    implicit val timout = akka.util.Timeout(1441 minutes)
    IO.fromFuture(
      IO.delay((fileServiceActor ? GetUri(mp)).asInstanceOf[Future[Uri]])
    )
  }

  def createSource(
      path: ManagedFilePath,
      fromOffset: Long
  ): Source[ByteString, _] = {
    val serviceactor = fileServiceActor
    implicit val timout = akka.util.Timeout(1441 minutes)
    val ac = context.actorOf(
      Props(new FileUserSource(path, serviceactor, fromOffset))
        .withDispatcher("fileuser-dispatcher")
    )

    val f = (ac ? WaitingForPath)
      .asInstanceOf[Future[Try[Source[ByteString, _]]]]

    val f2 = f map (_ match {
      case Success(r) => r
      case Failure(e) =>
        throw new RuntimeException("getSourceToFile failed. " + path, e)
    })

    Source.future(f2).flatMapConcat(x => x)
  }

  /* If size < 0 then it must not check the size and the hash
   *  but must return true iff the file is readable
   */
  def contains(
      path: ManagedFilePath,
      size: Long,
      hash: Int
  ): IO[Boolean] = {
    implicit val timout = akka.util.Timeout(1441 minutes)
    IO.fromFuture(
      IO.delay(
        (fileServiceActor ? IsAccessible(path, size, hash))
          .asInstanceOf[Future[Boolean]]
      )
    )
  }

  def contains(
      path: ManagedFilePath,
      retrieveSizeAndHash: Boolean
  ): IO[Option[SharedFile]] = {
    implicit val timout = akka.util.Timeout(1441 minutes)

    IO.fromFuture(
      IO.delay(
        ((fileServiceActor ? IsPathAccessible(path, retrieveSizeAndHash))
          .asInstanceOf[Future[Option[SharedFile]]])
      )
    )
  }

  def importFile(
      file: File,
      path: ProposedManagedFilePath
  ): IO[(Long, Int, ManagedFilePath)] = {
    if (!file.canRead) {
      throw new java.io.FileNotFoundException("not found" + file)
    }

    implicit val timout = akka.util.Timeout(1441 minutes)

    val ac = context.actorOf(
      Props(
        new FileSender(file, path, false, fileServiceActor)
      ).withDispatcher("filesender-dispatcher")
    )
    IO.fromFuture(
      IO.delay(
        ((ac ? WaitingForSharedFile)
          .asInstanceOf[Future[Option[SharedFile]]]
          .map(_.get)
          .andThen { case _ => ac ! PoisonPill }
          .map { sf =>
            val size = sf.byteSize
            val hash = sf.hash
            val managed = sf.path.asInstanceOf[ManagedFilePath]
            (size, hash, managed)
          })
      )
    )
  }

  def sink(
      path: ProposedManagedFilePath
  ): Sink[ByteString, Future[(Long, Int, ManagedFilePath)]] =
    SinkActor.make(path, fileServiceActor)(context, mat)

  def exportFile(path: ManagedFilePath): Resource[IO, File] = {
    Resource.make(this.contains(path, true).flatMap {
      case None => throw new RuntimeException("no such path")
      case Some(sf) =>
        val serviceactor = fileServiceActor
        implicit val timout = akka.util.Timeout(1441 minutes)
        val ac = context.actorOf(
          Props(new FileUser(path, sf.byteSize, sf.hash, serviceactor))
            .withDispatcher("fileuser-dispatcher")
        )

        IO.fromFuture(
          IO.delay {
            (ac ? WaitingForPath).asInstanceOf[Future[Try[File]]]
          }
        ).guarantee {
          IO { ac ! PoisonPill }
        } map (_ match {
          case Success(r) => r
          case Failure(e) =>
            throw new RuntimeException("getPathToFile failed. " + path, e)
        })

    })(file => IO(file.delete))
  }

  def sharedFolder(prefix: Seq[String]): IO[Option[File]] = {
    implicit val timout = akka.util.Timeout(1441 minutes)
    IO.fromFuture(
      IO.delay(
        (fileServiceActor ? GetSharedFolder(prefix.toVector))
          .mapTo[Option[File]]
      )
    )
  }

  def delete(
      path: ManagedFilePath,
      expectedSize: Long,
      expectedHash: Int
  ): IO[Boolean] = {
    implicit val timout = akka.util.Timeout(1441 minutes)
    IO.fromFuture(
      IO.delay(
        (fileServiceActor ? Delete(path, expectedSize, expectedHash))
          .asInstanceOf[Future[Boolean]]
      )
    )
  }

}
