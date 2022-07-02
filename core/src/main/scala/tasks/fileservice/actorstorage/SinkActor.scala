package tasks.fileservice.actorfilestorage

import tasks.fileservice._

import akka.actor._
import akka.stream.scaladsl._
import akka.stream.Materializer
import akka.util._

import tasks.wire._
import tasks.wire.filetransfermessages.EndChunk
import tasks.wire.filetransfermessages.CannotSaveFile
import tasks.wire.filetransfermessages.Chunk
import scala.concurrent.Future
import akka.stream.OverflowStrategy
import akka.stream.CompletionStrategy

object SinkActor {
  case object StreamInitialized
  case object StreamCompleted
  final case class StreamFailure(ex: Throwable)
  case object StreamAck
  def make(path: ProposedManagedFilePath, service: ActorRef)(implicit
      context: ActorRefFactory,
      mat: Materializer
  ): Sink[ByteString, Future[(Long, Int, ManagedFilePath)]] = {
    val (sharedFileActor, source) = Source
      .actorRef[(Long, Int, ManagedFilePath)](
        completionMatcher = { case akka.actor.Status.Success(()) =>
          akka.stream.CompletionStrategy.draining
        }: PartialFunction[Any, CompletionStrategy],
        failureMatcher = { case akka.actor.Status.Failure(ex) =>
          ex
        }: PartialFunction[Any, Throwable],
        bufferSize = 1,
        overflowStrategy = OverflowStrategy.dropHead
      )
      .preMaterialize()
    val sinkActor =
      context.actorOf(Props(new SinkActor(path, service, sharedFileActor)))
    val sink = Sink.actorRefWithBackpressure[ByteString](
      sinkActor,
      onInitMessage = StreamInitialized,
      onCompleteMessage = StreamCompleted,
      onFailureMessage = StreamFailure,
      ackMessage = StreamAck
    )
    Flow
      .fromSinkAndSource(sink, source)
      .toMat(Sink.head)((_, b) => b)
  }
}

class SinkActor(
    proposedPath: ProposedManagedFilePath,
    service: ActorRef,
    finalResultActor: ActorRef
) extends Actor
    with akka.actor.ActorLogging {

  implicit val mat = Materializer(context)

  override def preStart() = {
    service ! NewSource(proposedPath)
  }

  var sharedFile: Option[SharedFile] = None
  var transferin: Option[ActorRef] = None
  var streamActor: Option[ActorRef] = None

  def receive = {
    case SinkActor.StreamInitialized =>
      streamActor = Some(sender())
      if (transferin.isDefined) {
        sender() ! SinkActor.StreamAck
      }
    case SinkActor.StreamCompleted =>
      transferin.get ! EndChunk()

    case SinkActor.StreamFailure(ex) =>
      transferin.get ! CannotSaveFile(ex.getMessage())
      self ! PoisonPill
      finalResultActor ! ex

    case x: ByteString =>
      transferin.get ! Chunk(com.google.protobuf.ByteString.copyFrom(x.toArray))
    case _: tasks.wire.filetransfermessages.Ack =>
      streamActor.get ! SinkActor.StreamAck

    case t: SharedFile =>
      sharedFile = Some(t)
      finalResultActor ! (t.byteSize, t.hash, t.path
        .asInstanceOf[ManagedFilePath])
      finalResultActor ! akka.actor.Status.Success(())
      self ! PoisonPill

    case TransferToMe(transferin0) =>
      transferin = Some(transferin0)
      if (streamActor.isDefined) {
        streamActor.get ! SinkActor.StreamAck
      }

    case ErrorWhileAccessingStore(e) =>
      log.error("ErrorWhileAccessingStore: " + e)
      finalResultActor ! e
      self ! PoisonPill
  }

}
