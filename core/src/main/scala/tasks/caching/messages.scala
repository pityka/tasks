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

import tasks.util._
import tasks.util.eq._
import tasks.queue._
import tasks.fileservice._
import tasks._

import upickle.Js

import akka.actor.ActorRef

@SerialVersionUID(1L)
case class CacheActor(actor: ActorRef) extends Serializable

@SerialVersionUID(1L)
private[tasks] case class SaveResult(sch: TaskDescription,
                                     r: UntypedResult,
                                     prefix: FileServicePrefix)
    extends Serializable

@SerialVersionUID(1L)
private[tasks] case class CheckResult(sch: ScheduleTask, sender: ActorRef)
    extends Serializable

@SerialVersionUID(1L)
private[tasks] case object PoisonPillToCacheActor extends Serializable

@SerialVersionUID(1L)
private[tasks] case class TaskNotFoundInCache(v: Boolean = true)
    extends Serializable

@SerialVersionUID(1L)
private[tasks] case class AnswerFromCache(
    message: Either[TaskNotFoundInCache, Option[UntypedResult]],
    sender: ActorRef,
    sch: ScheduleTask)
    extends Serializable
