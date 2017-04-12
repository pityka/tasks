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

// Used by others to register an Actor for watching
case class WatchMe(ref: ActorRef)
case class Latch(c: CountDownLatch)

abstract class Reaper extends Actor with akka.actor.ActorLogging {

  // Keep track of what we're watching
  val watched = ArrayBuffer.empty[ActorRef]
  val latches = ArrayBuffer.empty[CountDownLatch]

  // Derivations need to implement this method.  It's the
  // hook that's called when everything's dead
  def allSoulsReaped(): Unit

  // Watch and check for termination
  final def receive = {
    case Latch(l) =>
      latches += l
    case WatchMe(ref) =>
      log.debug("Watching: " + ref)
      context.watch(ref) // This ensures that the Terminated message will come
      watched += ref
      sender ! true
    case Terminated(ref) =>
      watched -= ref
      if (watched.isEmpty) {
        allSoulsReaped()
        latches.foreach(_.countDown)
      }
  }
}

class CallbackReaper(f: => Unit) extends Reaper {
  def allSoulsReaped = f
}

class ProductionReaper extends Reaper {
  // Shutdown
  def allSoulsReaped(): Unit = {
    log.info("All souls reaped. Calling system.shutdown.")
    context.system.shutdown()
  }
}
