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
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

case class UIUntypedResult(files: Set[UISharedFile], data: Base64Data)

object UIUntypedResult {
  implicit val codec: JsonValueCodec[UIUntypedResult] =
    JsonCodecMaker.make
}

sealed trait UIFilePath
case class UIManagedFilePath(path: Vector[String]) extends UIFilePath {
  override def toString = path.mkString("/")
}
object UIManagedFilePath {
  implicit val codec: JsonValueCodec[UIManagedFilePath] =
    JsonCodecMaker.make
}
case class UIRemoteFilePath(uri: String) extends UIFilePath {
  override def toString = uri
}
object UIRemoteFilePath {
  implicit val codec: JsonValueCodec[UIRemoteFilePath] =
    JsonCodecMaker.make
}
object UIFilePath {
  implicit val codec: JsonValueCodec[UIFilePath] =
    JsonCodecMaker.make
}

case class UISharedFile(path: UIFilePath, byteSize: Long, hash: Int)
object UISharedFile {
  implicit val codec: JsonValueCodec[UISharedFile] =
    JsonCodecMaker.make
}
case class UIHistory(
    dependencies: List[UISharedFile],
    task: TaskId,
    timestamp: java.time.Instant,
    codeVersion: String
)
object UIHistory {
  implicit val codec: JsonValueCodec[UIHistory] =
    JsonCodecMaker.make
}

case class UILauncherActor(actorPath: String)
object UILauncherActor {
  implicit val codec: JsonValueCodec[UILauncherActor] =
    JsonCodecMaker.make
}

case class UIQueueState(
    queuedTasks: List[HashedTaskDescription],
    scheduledTasks: List[
      (HashedTaskDescription, (UILauncherActor, VersionedResourceAllocated))
    ],
    knownLaunchers: Set[UILauncherActor],
    negotiation: Option[(UILauncherActor, HashedTaskDescription)],
    failedTasks: List[
      (HashedTaskDescription, (UILauncherActor, VersionedResourceAllocated))
    ],
    completedTasks: Set[(TaskId, Int)],
    recoveredTasks: Set[(TaskId, Int)]
)

object UIQueueState {
  val empty = UIQueueState(Nil, Nil, Set(), None, Nil, Set(), Set())
  implicit val codec: JsonValueCodec[UIQueueState] =
    JsonCodecMaker.make
}

case class UIJobId(value: String)
object UIJobId {
  implicit val codec: JsonValueCodec[UIJobId] =
    JsonCodecMaker.make
}

case class UIAppState(
    running: Seq[(UIJobId, ResourceAvailable)],
    pending: Seq[(UIJobId, ResourceAvailable)],
    cumulativeRequested: Int
)

object UIAppState {

  val empty = UIAppState(Nil, Nil, 0)
  implicit val codec: JsonValueCodec[UIAppState] =
    JsonCodecMaker.make
}
