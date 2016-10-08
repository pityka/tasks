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

import akka.actor.{Actor, PoisonPill, ActorRef}
import akka.actor.Actor._
import scala.concurrent.Future
import java.lang.Class
import java.io.File

import scala.util._

import tasks.util._
import tasks.util.eq._
import tasks.queue._
import tasks.fileservice._

import upickle.default._

class TaskResultCache(val cacheMap: Cache, fileService: FileServiceActor)
    extends Actor
    with akka.actor.ActorLogging {

  implicit def fs: FileServiceActor = fileService

  override def preStart = {
    log.info(
        "Cache service starting. " + cacheMap.toString + s". config.global.verifySharedFileInCache: ${config.global.verifySharedFileInCache}. config.global.skipContentHashVerificationAfterCache: ${config.global.skipContentHashVerificationAfterCache}.")
  }

  override def postStop {
    cacheMap.shutDown
    log.info("TaskResultCache stopped.")
  }

  def receive = {
    case SaveResult(description, result, prefix) => {
      log.debug("SavingResult")
      try {
        cacheMap.set(description, result)(prefix)
      } catch {
        case x: java.io.NotSerializableException =>
          log.error("can't serialize: " + result.toString)
      }

      log.debug("save done")
    }
    case CheckResult(sch, originalSender) => {

      val res = cacheMap.get(sch.description)(sch.fileServicePrefix)

      if (res.isEmpty) {
        log.debug("Checking: {}. Not found in cache.", sch.description.taskId)
        sender ! AnswerFromCache(Left(TaskNotFoundInCache(true)),
                                 originalSender,
                                 sch)
      } else {
        if (!config.global.verifySharedFileInCache) {
          log.debug("Checking: {}. Got something (not verified).",
                    sch.description.taskId)
          sender ! AnswerFromCache(Right(res), originalSender, sch)
        } else {
          val verified = Try(res.get.files.forall(_.isAccessible))
          verified match {
            case Success(x) if x === false => {
              log.warning(
                  "Checking: {}. Got something ({}), but failed to verify after cache.",
                  sch.description.taskId,
                  res.get)
              sender ! AnswerFromCache(Left(TaskNotFoundInCache(true)),
                                       originalSender,
                                       sch)
            }
            case Failure(e) => {
              log.warning(
                  "Checking: {}. Got something ({}), but failed to verify after cache with error:{}.",
                  sch.description.taskId,
                  res.get,
                  e)
              sender ! AnswerFromCache(Left(TaskNotFoundInCache(true)),
                                       originalSender,
                                       sch)
            }
            case Success(x) if x === true => {
              log.debug("Checking: {}. Got something (verified).",
                        sch.description.taskId)
              sender ! AnswerFromCache(Right(res), originalSender, sch)
            }
          }

        }
      }

    }
  }
}
