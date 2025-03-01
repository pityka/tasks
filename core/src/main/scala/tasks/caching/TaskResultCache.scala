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
package tasks.caching
import tasks._
import tasks.fileservice._
import tasks.queue._
import tasks.wire._
import tasks.util.config.TasksConfig
import cats.effect.IO
import tasks.util.message.MessageData.ScheduleTask

private[tasks] case class AnswerFromCache(
    message: Either[String, Option[UntypedResult]],
    sender: Proxy,
    sch: ScheduleTask
)

private[tasks] class TaskResultCache(
    cacheMap: Cache,
    fileService: FileServiceComponent,
    config: TasksConfig
) {
  override def toString = s"TaskResultCache(cacheMap=$cacheMap,filService=$fileService)"
  def checkResult(
      scheduleTask: ScheduleTask,
      originalSender: Proxy
  ): IO[AnswerFromCache] = {
    val taskId = scheduleTask.description.taskId
    val queryFileServicePrefix =
      config.cachePath match {
        case None => scheduleTask.fileServicePrefix.append(taskId.id)
        case Some(path) =>
          path.append(taskId.id)
      }

    def lookup =
      cacheMap
        .get(scheduleTask.description)(queryFileServicePrefix)
        .handleError { case e =>
          scribe.error(e, "Error while looking up in cache")
          None
        }

    val answer = for {
      cacheLookup <- lookup
      answer <- cacheLookup match {
        case None =>
          scribe.debug(s"Checking: $taskId ${scheduleTask.description.dataHash}. Not found in cache.")
          IO.pure(
            AnswerFromCache(
              Left("TaskNotFoundInCache"),
              originalSender,
              scheduleTask
            )
          )
        case _ if !config.verifySharedFileInCache =>
          scribe.debug(s"Checking: $taskId ${scheduleTask.description.dataHash}. Got something (not verified).")
          IO.pure(
            AnswerFromCache(Right(cacheLookup), originalSender, scheduleTask)
          )
        case Some(cacheLookup) =>
          scribe.debug(
            s"Checking: $taskId ${scheduleTask.description.dataHash}. Got something $cacheLookup, verifying.."
          )
          val files = cacheLookup.files.toSeq.map(sf => (sf, true))
          val mutableFiles =
            cacheLookup.mutableFiles.toSeq.flatten.map(sf => (sf, false))
          IO
            .parTraverseN(config.parallelismOfCacheAccessibilityCheck)(
              files ++ mutableFiles
            ) { case (sf, checkContent) =>
              SharedFileHelper.isAccessible(sf, checkContent)(fileService)
            }
            .map(seq => (seq, seq.forall(identity)))
            .handleError { case e =>
              scribe.warn(
                s"Checking: $taskId ${scheduleTask.description.dataHash}. Got something ($cacheLookup), but failed to verify after cache with error: $e."
              )
              (Nil, false)
            }
            .map {
              case (accessibility, false) =>
                val inaccessibleFiles =
                  (files zip accessibility).filterNot(_._2)
                scribe.warn(
                  s"Checking: $taskId ${scheduleTask.description.dataHash}. Got something ($cacheLookup), but failed to verify after cache. Inaccessible files: ${inaccessibleFiles.size} : ${inaccessibleFiles
                      .mkString(", ")}"
                )
                AnswerFromCache(
                  Left("TaskNotFoundInCache"),
                  originalSender,
                  scheduleTask
                )
              case (_, true) =>
                scribe.debug(s"Checking: $taskId ${scheduleTask.description.dataHash}d. Got something (verified).")
                AnswerFromCache(
                  Right(Some(cacheLookup)),
                  originalSender,
                  scheduleTask
                )

            }
      }
    } yield answer
    answer.onError { case e: Exception =>
      IO {
        scribe.error(e, "Cache check failed")
      }
    }

  }
  def saveResult(
      sch: HashedTaskDescription,
      result: UntypedResult,
      prefixFromTask: FileServicePrefix
  ): IO[Unit] = {
    scribe.debug(s"SavingResult: ${sch.taskId} ${sch.dataHash}")

    val prefix = config.cachePath match {
      case None => prefixFromTask
      case Some(path) =>
        path.append(sch.taskId.id)
    }
    cacheMap
      .set(sch, result)(prefix)
      .void
      .handleError { case e =>
        IO { scribe.error(e, "Error while saving into cache") }
      }
  }
}
