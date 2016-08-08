/*
* The MIT License
*
* Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
* Group Fellay
* Copyright (c) 2016 Istvan Bartha
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

import akka.actor.{ Actor, PoisonPill, ActorRef }
import akka.actor.Actor._
import scala.concurrent.Future
import java.lang.Class
import java.io.File

import scala.util._

import tasks.util._
import tasks.util.eq._
import tasks.queue._
import tasks.fileservice._

@SerialVersionUID(1L)
case class CacheActor(actor: ActorRef) extends Serializable

@SerialVersionUID(1L)
private[tasks] case class SaveResult(sch: ScheduleTask, r: Result) extends Serializable

@SerialVersionUID(1L)
private[tasks] case class CheckResult(sch: ScheduleTask, sender: ActorRef) extends Serializable

@SerialVersionUID(1L)
private[tasks] case class TaskNotFoundInCache(v: Boolean = true) extends Serializable

@SerialVersionUID(1L)
private[tasks] case class AnswerFromCache(message: Either[TaskNotFoundInCache, Option[Result]], sender: ActorRef, sch: ScheduleTask) extends Serializable

class TaskResultCache(val cacheMap: Cache, fileService: FileServiceActor) extends Actor with akka.actor.ActorLogging {

  implicit def fs: FileServiceActor = fileService

  override def preStart = {
    log.info("Cache service starting. " + cacheMap.toString + s". config.verifySharedFileInCache: ${config.verifySharedFileInCache}. config.skipContentHashVerificationAfterCache: ${config.skipContentHashVerificationAfterCache}.")
  }

  override def postStop {
    cacheMap.shutDown
    log.info("TaskResultCache stopped.")
  }

  def receive = {
    case SaveResult(sch, result) => {
      log.debug("SavingResult")
      try {
        cacheMap.set(sch.description.persistent, result)
      } catch {
        case x: java.io.NotSerializableException => log.error("can't serialize: " + result.toString)
      }

      log.debug("save done")
    }
    case CheckResult(sch, originalSender) => {

      val res = cacheMap.get(sch.description.persistent)

      if (res.isEmpty) {
        log.debug("Checking: {}. Not found in cache.", sch.description.taskID)
        sender ! AnswerFromCache(Left(TaskNotFoundInCache(true)), originalSender, sch)
      } else {
        if (!config.verifySharedFileInCache) {
          log.debug("Checking: {}. Got something (not verified).", sch.description.taskID)
          sender ! AnswerFromCache(Right(res), originalSender, sch)
        } else {
          val verified = Try(res.get.verifyAfterCache)
          verified match {
            case Success(x) if x === false => {
              log.warning("Checking: {}. Got something ({}), but failed to verify after cache.", sch.description.taskID, res.get)
              sender ! AnswerFromCache(Left(TaskNotFoundInCache(true)), originalSender, sch)
            }
            case Failure(e) => {
              log.warning("Checking: {}. Got something ({}), but failed to verify after cache with error:{}.", sch.description.taskID, res.get, e)
              sender ! AnswerFromCache(Left(TaskNotFoundInCache(true)), originalSender, sch)
            }
            case Success(x) if x === true => {
              log.debug("Checking: {}. Got something (verified).", sch.description.taskID)
              sender ! AnswerFromCache(Right(res), originalSender, sch)
            }
          }

        }
      }

    }
  }
}
