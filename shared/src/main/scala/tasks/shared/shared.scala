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

package tasks.shared

sealed trait ResourceRequest extends Serializable

sealed trait ResourceAllocated[T <: ResourceRequest] extends Serializable

sealed trait ResourceAvailable[T <: ResourceRequest, A <: ResourceAllocated[T]] {
  def canFulfillRequest(r: T): Boolean

  def substract(r: A): ResourceAvailable[T, A]

  def addBack(r: A): ResourceAvailable[T, A]

  def maximum(r: T): A

  def empty: Boolean

}

@SerialVersionUID(1L)
case class CPUMemoryRequest(cpu: (Int, Int), memory: Int)
    extends ResourceRequest

@SerialVersionUID(1L)
object CPUMemoryRequest {

  def apply(cpu: Int, memory: Int): CPUMemoryRequest =
    CPUMemoryRequest((cpu, cpu), memory)

  implicit val writer = upickle.default.Writer[CPUMemoryRequest] {
    case t =>
      implicitly[upickle.default.Writer[(Int, Int, Int)]]
        .write(t.cpu._1, t.cpu._2, t.memory)
  }
  implicit val reader = upickle.default.Reader[CPUMemoryRequest] {
    case js: upickle.Js.Value =>
      val (a, b, c) =
        implicitly[upickle.default.Reader[(Int, Int, Int)]].read(js)
      CPUMemoryRequest((a, b), c)
  }
}

@SerialVersionUID(1L)
case class CPUMemoryAllocated(cpu: Int, memory: Int)
    extends ResourceAllocated[CPUMemoryRequest]

@SerialVersionUID(1L)
case class CPUMemoryAvailable(cpu: Int, memory: Int)
    extends ResourceAvailable[CPUMemoryRequest, CPUMemoryAllocated] {

  def canFulfillRequest(r: CPUMemoryRequest) =
    cpu >= r.cpu._1 && memory >= r.memory

  def substract(r: CPUMemoryRequest) = {
    val remainingCPU = math.max((cpu - r.cpu._2), 0)
    CPUMemoryAvailable(remainingCPU, memory - r.memory)
  }

  def substract(r: CPUMemoryAllocated) =
    CPUMemoryAvailable(cpu - r.cpu, memory - r.memory)

  def addBack(r: CPUMemoryAllocated) =
    CPUMemoryAvailable(cpu + r.cpu, memory + r.memory)

  def maximum(r: CPUMemoryRequest) = {
    val allocatedMemory = math.min(r.memory, memory)
    val allocatedCPU = math.min(cpu, r.cpu._2)
    CPUMemoryAllocated(allocatedCPU, allocatedMemory)
  }

  def empty = cpu == 0 || memory == 0
  def isEmpty = empty

}

@SerialVersionUID(1L)
case class RunningJobId(value: String) extends Serializable

@SerialVersionUID(1L)
case class PendingJobId(value: String) extends Serializable
