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

case class UIUntypedResult(files:Set[UISharedFile], data: Base64Data)

object UIUntypedResult {
      implicit val encoder : Encoder[UIUntypedResult] =
      deriveEncoder[UIUntypedResult]
    implicit val decoder : Decoder[UIUntypedResult]=
      deriveDecoder[UIUntypedResult]
}

sealed trait UIFilePath
case class UIManagedFilePath(path:Vector[String]) extends UIFilePath
object UIManagedFilePath {
      implicit val encoder : Encoder[UIManagedFilePath]=
      deriveEncoder[UIManagedFilePath]
    implicit val decoder: Decoder[UIManagedFilePath] =
      deriveDecoder[UIManagedFilePath]
}
case class UIRemoteFilePath(uri:String) extends UIFilePath
object UIRemoteFilePath {
      implicit val encoder =
      deriveEncoder[UIRemoteFilePath]
    implicit val decoder =
      deriveDecoder[UIRemoteFilePath]
}
object UIFilePath {
      implicit val encoder =
      deriveEncoder[UIFilePath]
    implicit val decoder =
      deriveDecoder[UIFilePath]
}

case class UISharedFile(path: UIFilePath, byteSize:Long,hash:Int)
object UISharedFile {
      implicit val encoder : Encoder[UISharedFile] =
      deriveEncoder[UISharedFile]
    implicit val decoder : Decoder[UISharedFile] =
      deriveDecoder[UISharedFile]
}
case class UIHistory(dependencies: List[UISharedFile], task: TaskId, timestamp: java.time.Instant, codeVersion:String)
object UIHistory {
      implicit val encoder =
      deriveEncoder[UIHistory]
    implicit val decoder =
      deriveDecoder[UIHistory]
}



case class UILauncherActor(actorPath: String)
object UILauncherActor {
  implicit val encoder: Encoder[UILauncherActor] =
    deriveEncoder[UILauncherActor]
  implicit val decoder: Decoder[UILauncherActor] =
    deriveDecoder[UILauncherActor]
}

case class UIQueueState(
    queuedTasks: List[TaskDescription],
    scheduledTasks: List[(TaskDescription,
                          (UILauncherActor, VersionedCPUMemoryAllocated))],
    knownLaunchers: Set[UILauncherActor],
    negotiation: Option[(UILauncherActor, TaskDescription)],
    failedTasks: List[(TaskDescription,
                          (UILauncherActor, VersionedCPUMemoryAllocated))],
    completedTasks: List[(TaskDescription,
                          (UILauncherActor, VersionedCPUMemoryAllocated), UIUntypedResult)],  
    recoveredTasks : List[(TaskDescription, UIUntypedResult)]                                              
)

object UIQueueState {
  val empty = UIQueueState(Nil, Nil, Set(), None, Nil,Nil, Nil)
  implicit val encoder: Encoder[UIQueueState] = deriveEncoder[UIQueueState]
  implicit val decoder: Decoder[UIQueueState] = deriveDecoder[UIQueueState]
}

case class UIJobId(value:String)
object UIJobId {
  implicit val encoder: Encoder[UIJobId] =
    deriveEncoder[UIJobId]
  implicit val decoder: Decoder[UIJobId] =
    deriveDecoder[UIJobId]
} 

case class UIAppState(                                           
      running: Seq[(UIJobId, CPUMemoryAvailable)],
      pending: Seq[(UIJobId, CPUMemoryAvailable)],
      cumulativeRequested: Int
)

object UIAppState {

  val empty = UIAppState(Nil, Nil, 0  )
  implicit val encoder: Encoder[UIAppState] = deriveEncoder[UIAppState]
  implicit val decoder: Decoder[UIAppState] = deriveDecoder[UIAppState]
}
