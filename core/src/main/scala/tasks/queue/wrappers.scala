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

import tasks._
import akka.actor._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import tasks.util._

@SerialVersionUID(1L)
case class NodeLocalCacheActor(actor: ActorRef) extends Serializable

@SerialVersionUID(1L)
case class QueueActor(actor: ActorRef) extends Serializable

@SerialVersionUID(1L)
case class LauncherActor(actor: ActorRef) extends Serializable

case class ProxyTaskActorRef[B, T](private[tasks] val actor: ActorRef) {

  // def ~>[C <: Prerequisitive[C], A](child: ProxyTaskActorRef[C, A])(
  //     implicit updater: UpdatePrerequisitive[C, T])
  //   : ProxyTaskActorRef[C, A] = {
  //   ProxyTask.addTarget(actor, child.actor, updater)
  //   child
  // }

  def ?(implicit ec: ExecutionContext) =
    ProxyTask
      .getBackResultFuture(actor, config.global.proxyTaskGetBackResult)
      .asInstanceOf[Future[T]]

  def ?(timeoutp: FiniteDuration)(implicit ec: ExecutionContext) =
    ProxyTask.getBackResultFuture(actor, timeoutp).asInstanceOf[Future[T]]

  // def <~[A](result: A)(implicit updater: UpdatePrerequisitive[B, A]): Unit =
  //   actor ! result

}
