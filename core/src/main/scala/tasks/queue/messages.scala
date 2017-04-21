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

package tasks.queue

import akka.actor._

import tasks._
import tasks.shared._

import upickle.Js

@SerialVersionUID(1L)
case class LookUp(s: String)

@SerialVersionUID(1L)
case class Save(s: String, v: Any)

@SerialVersionUID(1L)
private[tasks] case object ATaskWasForwarded

@SerialVersionUID(1L)
private[tasks] case class QueueInfo(q: Map[ScheduleTask, List[ActorRef]])

@SerialVersionUID(1L)
private[tasks] case object GetQueueInformation

@SerialVersionUID(1L)
private[tasks] case class QueryTask(sch: ScheduleTask, ac: ActorRef)
    extends Serializable

@SerialVersionUID(1L)
private[tasks] case class TaskDone(sch: ScheduleTask, result: UntypedResult)
    extends Serializable

@SerialVersionUID(1L)
private[tasks] case class TaskFailedMessageToQueue(sch: ScheduleTask,
                                                   cause: Throwable)
    extends Serializable

@SerialVersionUID(1L)
private[tasks] case class TaskFailedMessageToProxy(sch: ScheduleTask,
                                                   cause: Throwable)
    extends Serializable

@SerialVersionUID(1L)
private[tasks] case class AskForWork(resources: CPUMemoryAvailable)
    extends Serializable

@SerialVersionUID(1L)
case object HowLoadedAreYou extends Serializable

// case class AddTarget[A <: Prerequisitive[A], B](
//     target: ActorRef,
//     updater: UpdatePrerequisitive[A, B])

// case class SaveUpdater[A <: Prerequisitive[A], B](
//     updater: UpdatePrerequisitive[A, B])

// case object UpdaterSaved

case object GetBackResult

private[tasks] case class InternalMessageFromTask(actor: ActorRef,
                                                  result: UntypedResult)
    extends Serializable

private[tasks] case class InternalMessageTaskFailed(actor: ActorRef,
                                                    cause: Throwable)
    extends Serializable

private[tasks] case class FailureMessageFromProxyToProxy(cause: Throwable)

case class MessageFromTask(result: UntypedResult) extends Serializable

case object SaveDone

// private[tasks] case object NegotiationTimeout

@SerialVersionUID(1L)
private[tasks] case class Ack(allocated: CPUMemoryAllocated)

@SerialVersionUID(1L)
private[tasks] case class RegisterForNotification(actor: ActorRef)
    extends Serializable

@SerialVersionUID(1L)
private[tasks] case object GetMaximumSlots

@SerialVersionUID(1L)
private[tasks] case object GetAvailableSlots

@SerialVersionUID(1L)
case object Release extends Serializable
