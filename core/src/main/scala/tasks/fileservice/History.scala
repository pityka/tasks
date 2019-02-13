package tasks.fileservice

import io.circe.generic.semiauto._
import io.circe.{Encoder, Decoder}

sealed trait HistoryContext {
  def dependencies: List[History]
}
case class HistoryContextImpl(
    dependencies: List[History],
    task: History.TaskVersion,
    codeVersion: String,
    timestamp: java.time.Instant
) extends HistoryContext {
  def deduplicate = {
    def loop(cursor: HistoryContext,
             filesSeen: Set[SharedFile]): (HistoryContext, Set[SharedFile]) =
      cursor match {
        case cursor: HistoryContextImpl =>
          val (updatedFilesSeen, modifiedDependencyList) =
            cursor.dependencies.foldLeft((filesSeen, List.empty[History])) {
              case ((filesSeen, accumulator), nextElem) =>
                if (filesSeen.contains(nextElem.self))
                  (filesSeen, nextElem.copy(context = None) :: accumulator)
                else {
                  nextElem.context match {
                    case None =>
                      (filesSeen, nextElem :: accumulator)
                    case Some(context) =>
                      val (deduplicatedContext, fileSeenInContext) =
                        loop(context, filesSeen + nextElem.self)

                      (fileSeenInContext,
                       nextElem
                         .copy(context = Some(deduplicatedContext)) :: accumulator)
                  }

                }

            }

          (cursor.copy(dependencies = modifiedDependencyList.reverse),
           updatedFilesSeen)
        case NoHistory =>
          (NoHistory, Set.empty)

      }

    loop(this, Set())._1
  }
}

case object NoHistory extends HistoryContext {
  def dependencies = Nil
}

case class History(self: SharedFile, context: Option[HistoryContext]) {
  def deduplicate =
    copy(context = context.map {
      case ctx: HistoryContextImpl => ctx.deduplicate
      case NoHistory               => NoHistory
    })
}

object History {

  implicit val ordering: Ordering[History] = Ordering.by(_.self.toString)

  case class TaskVersion(taskID: String, taskVersion: Int)
  object TaskVersion {
    implicit val encoder: Encoder[TaskVersion] =
      deriveEncoder[TaskVersion]
    implicit val decoder: Decoder[TaskVersion] =
      deriveDecoder[TaskVersion]
  }

  implicit val encoder: Encoder[History] =
    deriveEncoder[History]
  implicit val decoder: Decoder[History] =
    deriveDecoder[History]

}

object HistoryContext {
  implicit val instantEncoder =
    Encoder.encodeLong.contramap[java.time.Instant](i => i.toEpochMilli)

  implicit val instantDecoder =
    Decoder.decodeLong.map(i => java.time.Instant.ofEpochMilli(i))

  implicit val encoder: Encoder[HistoryContext] =
    deriveEncoder[HistoryContext]
  implicit val decoder: Decoder[HistoryContext] =
    deriveDecoder[HistoryContext]
}
