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

import akka.actor._
import scala.concurrent._

package object queue {

  def updateHistoryOfComputationEnvironment[T](
      computationEnvironment: ComputationEnvironment,
      deserializedInputData: T,
      taskID: String,
      taskVersion: Int
  ): ComputationEnvironment = deserializedInputData match {
    case t: ResultWithSharedFiles =>
      val newHistory = fileservice.History(
        dependencies = t.files.toList,
        task = fileservice.History.TaskVersion(taskID, taskVersion),
        timestamp = java.time.Instant.now,
        codeVersion = computationEnvironment.components.tasksConfig.codeVersion
      )

      computationEnvironment.copy(
        components = computationEnvironment.components.copy(filePrefix =
          computationEnvironment.components.filePrefix.withHistory(newHistory)))
    case _ => computationEnvironment
  }

  type CompFun2 = Base64Data => ComputationEnvironment => Future[UntypedResult]

  def newTask[A, B](
      prerequisitives: B,
      resource: shared.VersionedCPUMemoryRequest,
      function: CompFun2,
      taskId: TaskId
  )(implicit components: TaskSystemComponents,
    writer1: Serializer[B],
    reader2: Deserializer[A]): ProxyTaskActorRef[B, A] = {
    implicit val queue = components.queue
    implicit val fileService = components.fs
    implicit val cache = components.cache
    implicit val context = components.actorsystem
    implicit val prefix = components.filePrefix

    val taskId1 = taskId

    ProxyTaskActorRef[B, A](
      context.actorOf(
        Props(
          new ProxyTask[B, A](
            taskId = taskId1,
            runTaskClass = function.getClass,
            input = prerequisitives,
            writer = writer1,
            reader = reader2,
            resourceConsumed = resource,
            queueActor = queue.actor,
            fileServiceComponent = fileService,
            fileServicePrefix = prefix,
            cacheActor = cache.actor
          )
        ).withDispatcher("proxytask-dispatcher")
      )
    )
  }
}
