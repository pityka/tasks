/*
 * The MIT License
 *
 * Modified work, Copyright (c) 2018 Istvan Bartha
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
package tasks.ui

import tasks.queue.TaskQueue
import tasks.elastic.NodeRegistry
import tasks.util.reflectivelyInstantiateObject
import tasks.util.config.TasksConfig
import cats.effect.kernel.Resource
import cats.effect.IO

trait EventListener[-E] {
  def receive(event: E): Unit
  def close(): Unit
}

trait UIComponentBootstrap {
  def startQueueUI(implicit
      config: TasksConfig
  ): Resource[IO, QueueUI]
  def startAppUI(implicit config: TasksConfig): Resource[IO, AppUI]
}

trait QueueUI {
  def tasksQueueEventListener: EventListener[TaskQueue.Event]
}

trait AppUI {
  def nodeRegistryEventListener: EventListener[NodeRegistry.Event]
}

object UIComponentBootstrap {
  def load(implicit config: TasksConfig): Option[UIComponentBootstrap] =
    config.uiFqcn match {
      case ""     => None
      case "NOUI" => None
      case "default" =>
        Some(
          reflectivelyInstantiateObject[UIComponentBootstrap](
            "tasks.ui.BackendUIBootstrap"
          )
        )
      case other =>
        Some(reflectivelyInstantiateObject[UIComponentBootstrap](other))
    }
}
