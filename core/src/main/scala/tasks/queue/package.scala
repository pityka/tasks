/*
 * The MIT License
 *
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

package tasks
import tasks._

import upickle.default._
import upickle.Js
import akka.actor._
import scala.concurrent._

package object queue {

  type CompFun2 = Js.Value => ComputationEnvironment => Future[UntypedResult]

  def newTask[A, B](
      prerequisitives: B,
      resource: CPUMemoryRequest = CPUMemoryRequest(cpu = 1, memory = 500),
      f: CompFun2,
      taskId: TaskId
  )(implicit components: TaskSystemComponents,
    writer1: Writer[B],
    reader2: Reader[A]): ProxyTaskActorRef[B, A] = {
    implicit val queue = components.queue
    implicit val fileService = components.fs
    implicit val cache = components.cache
    implicit val context = components.actorsystem
    implicit val prefix = components.filePrefix

    val taskId1 = taskId

    ProxyTaskActorRef[B, A](
        context.actorOf(
            Props(
                new ProxyTask[B, A](taskId1,
                                    f.getClass,
                                    prerequisitives,
                                    writer1,
                                    reader2,
                                    resource,
                                    queue.actor,
                                    fileService,
                                    prefix,
                                    cache.actor)
            ).withDispatcher("proxytask-dispatcher")
        )
    )
  }
}
