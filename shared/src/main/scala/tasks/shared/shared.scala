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

import io.circe._
import io.circe.generic.semiauto._

sealed trait ResourceRequest extends Serializable

sealed trait ResourceAllocated[T <: ResourceRequest] extends Serializable

sealed trait ResourceAvailable[T <: ResourceRequest, A <: ResourceAllocated[T]] {
  def canFulfillRequest(r: T): Boolean

  def substract(r: T): ResourceAvailable[T, A]

  def substract(r: A): ResourceAvailable[T, A]

  def addBack(r: A): ResourceAvailable[T, A]

  def maximum(r: T): A

  def empty: Boolean

}

case class CPUMemoryRequest(cpu: (Int, Int), memory: Int)
    extends ResourceRequest

object CPUMemoryRequest {

  def apply(cpu: Int, memory: Int): CPUMemoryRequest =
    CPUMemoryRequest((cpu, cpu), memory)

  implicit val decoder: Decoder[CPUMemoryRequest] =
    deriveDecoder[CPUMemoryRequest]
  implicit val encoder: Encoder[CPUMemoryRequest] =
    deriveEncoder[CPUMemoryRequest]
}

case class CPUMemoryAllocated(cpu: Int, memory: Int)
    extends ResourceAllocated[CPUMemoryRequest]

object CPUMemoryAllocated {
  implicit val decoder: Decoder[CPUMemoryAllocated] =
    deriveDecoder[CPUMemoryAllocated]
  implicit val encoder: Encoder[CPUMemoryAllocated] =
    deriveEncoder[CPUMemoryAllocated]
}

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

object CPUMemoryAvailable {
  implicit val decoder: Decoder[CPUMemoryAvailable] =
    deriveDecoder[CPUMemoryAvailable]
  implicit val encoder: Encoder[CPUMemoryAvailable] =
    deriveEncoder[CPUMemoryAvailable]
}

case class VersionedCPUMemoryRequest(codeVersion: CodeVersion,
                                     cpuMemoryRequest: CPUMemoryRequest)
    extends ResourceRequest {
  def cpu = cpuMemoryRequest.cpu
  def memory = cpuMemoryRequest.memory
}

object VersionedCPUMemoryRequest {
  def apply(codeVersion: CodeVersion,
            cpu: Int,
            memory: Int): VersionedCPUMemoryRequest =
    VersionedCPUMemoryRequest(codeVersion, CPUMemoryRequest((cpu, cpu), memory))

  implicit val decoder: Decoder[VersionedCPUMemoryRequest] =
    deriveDecoder[VersionedCPUMemoryRequest]
  implicit val encoder: Encoder[VersionedCPUMemoryRequest] =
    deriveEncoder[VersionedCPUMemoryRequest]
}

case class VersionedCPUMemoryAllocated(codeVersion: CodeVersion,
                                       cpuMemoryAllocated: CPUMemoryAllocated)
    extends ResourceAllocated[VersionedCPUMemoryRequest] {
  def cpu = cpuMemoryAllocated.cpu
  def memory = cpuMemoryAllocated.memory
}

object VersionedCPUMemoryAllocated {
  implicit val decoder: Decoder[VersionedCPUMemoryAllocated] =
    deriveDecoder[VersionedCPUMemoryAllocated]
  implicit val encoder: Encoder[VersionedCPUMemoryAllocated] =
    deriveEncoder[VersionedCPUMemoryAllocated]
}

case class VersionedCPUMemoryAvailable(codeVersion: CodeVersion,
                                       cpuMemoryAvailable: CPUMemoryAvailable)
    extends ResourceAvailable[VersionedCPUMemoryRequest,
                              VersionedCPUMemoryAllocated] {
  def canFulfillRequest(r: VersionedCPUMemoryRequest) =
    r.codeVersion == codeVersion && cpuMemoryAvailable.canFulfillRequest(
      r.cpuMemoryRequest)

  def substract(r: VersionedCPUMemoryRequest) =
    VersionedCPUMemoryAvailable(
      codeVersion,
      cpuMemoryAvailable.substract(r.cpuMemoryRequest))

  def substract(r: VersionedCPUMemoryAllocated) =
    VersionedCPUMemoryAvailable(
      codeVersion,
      cpuMemoryAvailable.substract(r.cpuMemoryAllocated))

  def addBack(r: VersionedCPUMemoryAllocated) =
    VersionedCPUMemoryAvailable(
      codeVersion,
      cpuMemoryAvailable.addBack(r.cpuMemoryAllocated))

  def maximum(r: VersionedCPUMemoryRequest) =
    VersionedCPUMemoryAllocated(codeVersion,
                                cpuMemoryAvailable.maximum(r.cpuMemoryRequest))

  def empty = cpuMemoryAvailable.empty

}

object VersionedCPUMemoryAvailable {
  implicit val decoder: Decoder[VersionedCPUMemoryAvailable] =
    deriveDecoder[VersionedCPUMemoryAvailable]
  implicit val encoder: Encoder[VersionedCPUMemoryAvailable] =
    deriveEncoder[VersionedCPUMemoryAvailable]
}

case class RunningJobId(value: String)

object RunningJobId {
  implicit val decoder: Decoder[RunningJobId] =
    deriveDecoder[RunningJobId]
  implicit val encoder: Encoder[RunningJobId] =
    deriveEncoder[RunningJobId]
}

case class PendingJobId(value: String)

object PendingJobId {
  implicit val decoder: Decoder[PendingJobId] = deriveDecoder[PendingJobId]
  implicit val encoder: Encoder[PendingJobId] = deriveEncoder[PendingJobId]
}
