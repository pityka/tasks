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

import tasks.shared._
import tasks.queue._
import io.circe._
import io.circe.generic.semiauto._

case class UILauncherActor(actorPath: String)
object UILauncherActor {
  implicit val encoder: Encoder[UILauncherActor] =
    deriveEncoder[UILauncherActor]
  implicit val decoder: Decoder[UILauncherActor] =
    deriveDecoder[UILauncherActor]
}

case class UIState(
    queuedTasks: List[TaskDescription],
    scheduledTasks: List[(TaskDescription,
                          (UILauncherActor, VersionedCPUMemoryAllocated))],
    knownLaunchers: Set[UILauncherActor],
    negotiation: Option[(UILauncherActor, TaskDescription)],
    failedTasks: List[(TaskDescription,
                          (UILauncherActor, VersionedCPUMemoryAllocated))],
    completedTasks: List[(TaskDescription,
                          (UILauncherActor, VersionedCPUMemoryAllocated))],                          
)

object UIState {
  val empty = UIState(Nil, Nil, Set(), None, Nil,Nil)
  implicit val encoder: Encoder[UIState] = deriveEncoder[UIState]
  implicit val decoder: Decoder[UIState] = deriveDecoder[UIState]
}
