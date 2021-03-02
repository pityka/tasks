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

package tasks.util

import java.util.concurrent.CountDownLatch
import akka.actor.{Actor, ActorRef, Terminated}
import scala.collection.mutable.ArrayBuffer

case class WatchMe(ref: ActorRef, answer: Boolean = false)
case class Latch(c: CountDownLatch)

abstract class Reaper extends Actor with akka.actor.ActorLogging {

  private val watched = ArrayBuffer.empty[ActorRef]
  private val latches = ArrayBuffer.empty[CountDownLatch]

  def allSoulsReaped(): Unit

  final def receive = {
    case Latch(l) =>
      latches += l
      log.info("Latch received. Watched actor refs: " + watched)
    case WatchMe(ref, answer) =>
      log.info("Watching: " + ref)
      context.watch(ref)
      watched += ref
      if (answer) {
        sender() ! true
      }
    case Terminated(ref) =>
      watched -= ref
      log.info("Terminated: " + ref)
      if (watched.isEmpty) {
        allSoulsReaped()
        latches.foreach(_.countDown)
      }
    case e =>
      log.warning("Unhandled: " + e)
  }
}

class CallbackReaper(f: => Unit) extends Reaper {
  def allSoulsReaped() = f
}

class ShutdownActorSystemReaper extends Reaper {
  def allSoulsReaped() = {
    log.info("All souls reaped. Calling system.shutdown.")
    context.system.terminate()
  }
}
