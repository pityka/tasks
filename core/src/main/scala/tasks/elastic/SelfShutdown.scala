/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
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

package tasks.elastic

import akka.actor.{Actor, ActorLogging}

import tasks.shared._
import tasks.util._
import tasks.util.config._
import tasks.queue.QueueActor

class SelfShutdown(shutdownRunningNode: ShutdownRunningNode,
                   id: RunningJobId,
                   queueActor: QueueActor)(implicit config: TasksConfig)
    extends Actor
    with ActorLogging {

  private case object QueueLostOrStopped

  def shutdown() = {
    shutdownRunningNode.shutdownRunningNode(id)
  }

  override def preStart: Unit = {
    HeartBeatActor.watch(queueActor.actor, QueueLostOrStopped, self)
    context.system.eventStream
      .subscribe(self, classOf[akka.remote.DisassociatedEvent])

  }
  def receive = {
    case QueueLostOrStopped =>
      log.error("QueueLostOrStopped received. Shutting down.")
      shutdown

    case de: akka.remote.DisassociatedEvent =>
      log.error(
        "DisassociatedEvent. " + de.remoteAddress + " vs " + queueActor.actor.path.address)
      if (de.remoteAddress == queueActor.actor.path.address) {
        shutdown
      }

  }
}