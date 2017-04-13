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

package tasks.simpletask

import scala.io.Source
import akka.actor.{Actor, PoisonPill, ActorRef, Props}
import akka.actor.Actor._
import scala.concurrent._
import scala.concurrent.duration._

import tasks._
import tasks.queue._
import tasks.caching._
import tasks.fileservice._

import upickle.default._

case class IntResult(val value: Int)

object SimpleTask {

  case class MyResultSet(val num: Option[Int], val id: Option[Int])

  val runTask: CompFun2 = { js => implicit env =>
    Future {
      val rs = implicitly[Reader[MyResultSet]].read(js)
      // Logger.debug( "task implementation started" + rs.num.toString)
      log.warning("boo")
      NodeLocalCache
        .getItem("asdfsdaf") {
          "value"
        }
        .map { x =>
          Thread.sleep(50)

          if (rs.num.get == 42) throw new RuntimeException("failtest")

          // Logger.debug( "task implementation ended")
          // Logger.debug( rs.num.get)

          UntypedResult(Set(), JsonString(write(new IntResult(rs.num.get))))
        }

    }.flatMap(x => x)

  }

  def spawn(counter: Int, id: Int = 0)(
      implicit components: TaskSystemComponents,
      writer1: Writer[MyResultSet],
      reader2: Reader[IntResult]) =
    newTask[IntResult, MyResultSet](
        prerequisitives = MyResultSet(Some(counter), Some(id)),
        resource = CPUMemoryRequest(cpu = 1, memory = 500),
        f = runTask,
        taskId = TaskId(runTask.getClass.getName, 1)
    ).actor

}

// class SimpleTask(
//     var counter: Int,
//     id: Int = 0
// )(implicit queue: QueueActor,
//   fileService: FileServiceActor,
//   prefix: FileServicePrefix,
//   cache: CacheActor)
//     extends ProxyTask(queue.actor, fileService.actor, prefix, cache.actor) {
//   import SimpleTask._
//
//   type MyPrerequisitive = SimpleTask.MyResultSet
//
//   type MyResult = IntResult
//
//   val runTaskClass = SimpleTask.runTask.getClass
//
//   val incomings = MyResultSet(Some(counter), Some(id))
//
//   val taskId = TaskId(runTaskClass.getName, 1)
//
//   val writer = implicitly[Writer[MyPrerequisitive]]
//
//   val reader = implicitly[Reader[MyResult]]
//
//   def emptyResultSet =
//     if (counter > 0) MyResultSet(Some(counter), Some(id))
//     else MyResultSet(None, Some(id)).asInstanceOf[MyResultSet]
//
// }
