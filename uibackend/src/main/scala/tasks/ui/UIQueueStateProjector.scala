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

import tasks.queue._
import tasks.queue.TaskQueue._

object UIQueueStateProjector {

  private def uiLauncherActor(launcher: LauncherActor) =
    UILauncherActor(launcher.actor.path.toString)

  def project(state: UIQueueState,
              taskQueueEvent: TaskQueue.Event): UIQueueState = {
    val scheduledTasksMap = state.scheduledTasks.toMap
    taskQueueEvent match {
      case Enqueued(sch, _) =>
        if (!scheduledTasksMap.contains(sch.description)) {
          state.copy(queuedTasks = sch.description :: state.queuedTasks)
        } else state

      case _: ProxyAddedToScheduledMessage =>
        state
      case Negotiating(launcher, sch) =>
        state.copy(
          negotiation = Some((uiLauncherActor(launcher), sch.description)))
      case LauncherJoined(launcher) =>
        state.copy(
          knownLaunchers = state.knownLaunchers + uiLauncherActor(launcher))
      case NegotiationDone => state.copy(negotiation = None)
      case TaskScheduled(sch, launcher, allocated) =>
        state.copy(
          queuedTasks = state.queuedTasks.filterNot(_ == sch.description),
          scheduledTasks =
            (sch.description, (uiLauncherActor(launcher), allocated)) :: state.scheduledTasks
        )

      case TaskDone(sch, _, _, _) =>
        val updatedCompletedTasks = {
          val scheduled = state.scheduledTasks
            .find(_._1 == sch.description)
            .isDefined
          if (scheduled) {
            val map = state.completedTasks.toMap
            val key = sch.description.taskId
            val updatedMap = map.get(key) match {
              case None        => map.updated(key, 1)
              case Some(count) => map.updated(key, count + 1)
            }
            updatedMap.toSet
          } else state.completedTasks
        }

        state.copy(scheduledTasks =
                     state.scheduledTasks.filterNot(_._1 == sch.description),
                   completedTasks = updatedCompletedTasks)
      case TaskFailed(sch) =>
        val updatedFailedTasks = state.scheduledTasks.filter(
          _._1 == sch.description) ::: state.failedTasks

        state.copy(scheduledTasks =
                     state.scheduledTasks.filterNot(_._1 == sch.description),
                   failedTasks = updatedFailedTasks)
      case TaskLauncherStoppedFor(sch) =>
        state.copy(
          scheduledTasks =
            state.scheduledTasks.filterNot(_._1 == sch.description))
      case LauncherCrashed(launcher) =>
        state.copy(
          knownLaunchers =
            state.knownLaunchers.filterNot(_ == uiLauncherActor(launcher)))
      case _: CacheQueried => state
      case CacheHit(sch, _) =>
        val updatedRecoveredTasks = {
          val map = state.recoveredTasks.toMap
          val key = sch.description.taskId
          val updatedMap = map.get(key) match {
            case None        => map.updated(key, 1)
            case Some(count) => map.updated(key, count + 1)
          }
          updatedMap.toSet
        }
        state.copy(recoveredTasks = updatedRecoveredTasks)

    }
  }
}
