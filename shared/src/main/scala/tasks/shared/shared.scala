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
import java.time.Instant

case class ResourceRequest(cpu: (Int, Int), memory: Int, scratch: Int) {
  def toAvailable = ResourceAvailable(cpu._2, memory, scratch)
}

object ResourceRequest {

  def apply(cpu: Int, memory: Int, scratch: Int): ResourceRequest =
    ResourceRequest((cpu, cpu), memory, scratch)

  implicit val decoder: Decoder[ResourceRequest] =
    deriveDecoder[ResourceRequest]
  implicit val encoder: Encoder[ResourceRequest] =
    deriveEncoder[ResourceRequest]
}

case class ResourceAllocated(cpu: Int, memory: Int, scratch: Int) {
  def toRequest = ResourceRequest(cpu, memory, scratch)
}

object ResourceAllocated {
  implicit val decoder: Decoder[ResourceAllocated] =
    deriveDecoder[ResourceAllocated]
  implicit val encoder: Encoder[ResourceAllocated] =
    deriveEncoder[ResourceAllocated]
}

case class ResourceAvailable(cpu: Int, memory: Int, scratch: Int) {

  def canFulfillRequest(r: ResourceRequest) =
    cpu >= r.cpu._1 && memory >= r.memory && scratch >= r.scratch

  def substract(r: ResourceRequest) = {
    val remainingCPU = math.max((cpu - r.cpu._2), 0)
    ResourceAvailable(remainingCPU, memory - r.memory, scratch - r.scratch)
  }

  def substract(r: ResourceAllocated) =
    ResourceAvailable(cpu - r.cpu, memory - r.memory, scratch - r.scratch)

  def addBack(r: ResourceAllocated) =
    ResourceAvailable(cpu + r.cpu, memory + r.memory, scratch + r.scratch)

  def maximum(r: ResourceRequest) = {
    val allocatedMemory = math.min(r.memory, memory)
    val allocatedCPU = math.min(cpu, r.cpu._2)
    val allocatedScratch = math.min(scratch, r.scratch)
    ResourceAllocated(allocatedCPU, allocatedMemory, allocatedScratch)
  }

  def empty = cpu == 0 || memory == 0 || scratch == 0
  def isEmpty = empty

}

object ResourceAvailable {
  implicit val decoder: Decoder[ResourceAvailable] =
    deriveDecoder[ResourceAvailable]
  implicit val encoder: Encoder[ResourceAvailable] =
    deriveEncoder[ResourceAvailable]
}

case class VersionedResourceRequest(
    codeVersion: CodeVersion,
    cpuMemoryRequest: ResourceRequest
) {
  def cpu = cpuMemoryRequest.cpu
  def memory = cpuMemoryRequest.memory
  def scratch = cpuMemoryRequest.scratch
}

object VersionedResourceRequest {
  def apply(
      codeVersion: CodeVersion,
      cpu: Int,
      memory: Int,
      scratch: Int
  ): VersionedResourceRequest =
    VersionedResourceRequest(
      codeVersion,
      ResourceRequest((cpu, cpu), memory, scratch)
    )

  implicit val decoder: Decoder[VersionedResourceRequest] =
    deriveDecoder[VersionedResourceRequest]
  implicit val encoder: Encoder[VersionedResourceRequest] =
    deriveEncoder[VersionedResourceRequest]
}

case class VersionedResourceAllocated(
    codeVersion: CodeVersion,
    cpuMemoryAllocated: ResourceAllocated
) {
  def cpu = cpuMemoryAllocated.cpu
  def memory = cpuMemoryAllocated.memory
  def scratch = cpuMemoryAllocated.scratch
}

object VersionedResourceAllocated {
  implicit val decoder: Decoder[VersionedResourceAllocated] =
    deriveDecoder[VersionedResourceAllocated]
  implicit val encoder: Encoder[VersionedResourceAllocated] =
    deriveEncoder[VersionedResourceAllocated]
}

case class VersionedResourceAvailable(
    codeVersion: CodeVersion,
    cpuMemoryAvailable: ResourceAvailable
) {
  def canFulfillRequest(r: VersionedResourceRequest) =
    r.codeVersion == codeVersion && cpuMemoryAvailable.canFulfillRequest(
      r.cpuMemoryRequest
    )

  def substract(r: VersionedResourceRequest) =
    VersionedResourceAvailable(
      codeVersion,
      cpuMemoryAvailable.substract(r.cpuMemoryRequest)
    )

  def substract(r: VersionedResourceAllocated) =
    VersionedResourceAvailable(
      codeVersion,
      cpuMemoryAvailable.substract(r.cpuMemoryAllocated)
    )

  def addBack(r: VersionedResourceAllocated) =
    VersionedResourceAvailable(
      codeVersion,
      cpuMemoryAvailable.addBack(r.cpuMemoryAllocated)
    )

  def maximum(r: VersionedResourceRequest) =
    VersionedResourceAllocated(
      codeVersion,
      cpuMemoryAvailable.maximum(r.cpuMemoryRequest)
    )

  def empty = cpuMemoryAvailable.empty

}

object VersionedResourceAvailable {
  implicit val decoder: Decoder[VersionedResourceAvailable] =
    deriveDecoder[VersionedResourceAvailable]
  implicit val encoder: Encoder[VersionedResourceAvailable] =
    deriveEncoder[VersionedResourceAvailable]
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

case class Labels(values: List[(String, String)]) {
  def ++(other: Labels) = Labels(values ++ other.values)

}
object Labels {
  implicit val decoder: Decoder[Labels] = deriveDecoder[Labels]
  implicit val encoder: Encoder[Labels] = deriveEncoder[Labels]
  val empty = Labels(Nil)
}

case class LogRecord(
    data: String,
    timestamp: Instant
)

object LogRecord {
  implicit val decoder: Decoder[LogRecord] = deriveDecoder[LogRecord]
  implicit val encoder: Encoder[LogRecord] = deriveEncoder[LogRecord]
}
