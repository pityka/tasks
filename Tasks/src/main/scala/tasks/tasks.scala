/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Copyright (c) 2016 Istvan Bartha
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

import tasks.queue._
import tasks.shared._
import tasks.fileservice._

import akka.actor._

// This is the output of a task
trait Result extends Serializable {
  def verifyAfterCache(implicit service: FileServiceActor,
                       context: ActorRefFactory): Boolean = true
}

class TaskDefinition[A <: Prerequisitive[A], B <: Result](
    val computation: CompFun[A, B],
    val taskID: String) {

  def this(computation: CompFun[A, B]) =
    this(computation, computation.getClass.getName)

  def apply(a: A)(resource: CPUMemoryRequest)(
      implicit components: TaskSystemComponents): ProxyTaskActorRef[A, B] =
    newTask[B, A](a, resource, computation, taskID)

}

case class UpdatePrerequisitive[A <: Prerequisitive[A], B <: Result](
    pf: PartialFunction[(A, B), A])
    extends PartialFunction[(A, B), A] {
  def apply(v1: (A, B)) = pf.apply(v1)
  def isDefinedAt(x: (A, B)) = pf.isDefinedAt(x)
}

sealed trait GridEngine
case object EC2Grid extends GridEngine {
  override def toString = "EC2"
}
case object NoGrid extends GridEngine {
  override def toString = "NOENGINE"
}
case object SSHGrid extends GridEngine {
  override def toString = "SSH"
}

case class STP1[A1](a1: Option[A1]) extends SimplePrerequisitive[STP1[A1]]
case class STP2[A1, A2](a1: Option[A1], a2: Option[A2])
    extends SimplePrerequisitive[STP2[A1, A2]]
