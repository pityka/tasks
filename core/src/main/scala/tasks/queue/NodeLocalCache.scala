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

package tasks.queue

import akka.actor.{Actor, ActorRef, ActorRefFactory, Props}
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import tasks.TaskSystemComponents
import tasks.Implicits._
import tasks.wire._

object NodeLocalCache {
  def start(implicit AS: ActorRefFactory) =
    NodeLocalCacheActor(
      AS.actorOf(Props[NodeLocalCache].withDispatcher("my-pinned-dispatcher")))

  def getItemAsync[A](key: String)(orElse: => Future[A])(
      implicit tsc: TaskSystemComponents): Future[A] =
    _getItemAsync(key)(orElse)

  def getItem[A](key: String)(orElse: => A)(
      implicit tsc: TaskSystemComponents): Future[A] = _getItem(key)(orElse)

  def drop(key: String)(implicit tsc: TaskSystemComponents) =
    tsc.nodeLocalCache.actor ! Drop(key)

  private[tasks] def _getItemAsync[A](key: String)(orElse: => Future[A])(
      implicit nlc: NodeLocalCacheActor,
      ec: ExecutionContext): Future[A] =
    _getItem(key)(orElse).flatMap(identity)

  private[tasks] def _getItem[A](key: String)(orElse: => A)(
      implicit nlc: NodeLocalCacheActor,
      ec: ExecutionContext): Future[A] = {
    implicit val to = akka.util.Timeout(168 hours)
    (nlc.actor ? LookUp(key)).map {
      case YouShouldSetIt => {
        val computed = orElse
        nlc.actor ! Save(key, computed)
        computed
      }
      case other => other.asInstanceOf[A]

    }
  }

  private class NodeLocalCache extends Actor with akka.actor.ActorLogging {
    private val map = scala.collection.mutable.Map[String, Option[Any]]()

    private val waitingList =
      scala.collection.mutable.ListBuffer[(String, ActorRef)]()

    def receive = {
      case LookUp(key) =>
        map.get(key) match {
          case None =>
            log.debug(s"LookUp($key): Not Found. Reply with YouShouldSetIt")
            map += key -> None
            sender ! YouShouldSetIt
          case Some(Some(hit)) =>
            log.debug(s"LookUp($key): Found. Reply with item.")
            sender ! hit
          case Some(None) =>
            log.debug(
              s"LookUp($key): Item is under production, adding sender to waiting list.")
            waitingList += key -> sender
        }

      case Save(key, value) =>
        log.debug(s"Save($key): Distributing to waiting list.")
        map += key -> Some(value)
        waitingList.filter(_._1 == key).map(_._2).foreach(_ ! value)

      case Drop(key) =>
        log.debug(s"Drop $key")
        map -= key
    }

  }
}
