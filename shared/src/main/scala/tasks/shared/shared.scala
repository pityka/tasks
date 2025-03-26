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

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import java.time.Instant

case class ResourceRequest(
    cpu: (Int, Int),
    memory: Int,
    scratch: Int,
    gpu: Int,
    image: Option[String]
)

object ResourceRequest {

  def apply(cpu: Int, memory: Int, scratch: Int, gpu: Int): ResourceRequest =
    ResourceRequest((cpu, cpu), memory, scratch, gpu, None)

  implicit val codec: JsonValueCodec[ResourceRequest] = JsonCodecMaker.make

}

case class ResourceAllocated(
    cpu: Int,
    memory: Int,
    scratch: Int,
    gpu: List[Int],
    image: Option[String]
) {
  val gpuWithMultiplicities = ResourceAvailable.zipMultiplicities(gpu)
}

object ResourceAllocated {
  implicit val codec: JsonValueCodec[ResourceAllocated] = JsonCodecMaker.make
}

case class ResourceAvailable(
    cpu: Int,
    memory: Int,
    scratch: Int,
    gpu: List[Int],
    image: Option[String]
) {

  val gpuWithMultiplicities = ResourceAvailable.zipMultiplicities(gpu)
  val numGpu = gpu.size

  def canFulfillRequest(r: ResourceAllocated) =
    cpu >= r.cpu && memory >= r.memory && scratch >= r.scratch &&
      r.gpuWithMultiplicities
        .forall(g => gpuWithMultiplicities.contains(g))

  def canFulfillRequest(r: ResourceRequest) =
    cpu >= r.cpu._1 && memory >= r.memory && scratch >= r.scratch && numGpu >= r.gpu && r.image
      .map(requestedImage => image.exists(_ == requestedImage))
      .getOrElse(true)

  def substract(r: ResourceRequest) = {
    val remainingCPU = math.max((cpu - r.cpu._2), 0)
    ResourceAvailable(
      remainingCPU,
      memory - r.memory,
      scratch - r.scratch,
      gpu = gpu.drop(r.gpu),
      image = image
    )
  }

  def substract(r: ResourceAllocated) =
    ResourceAvailable(
      cpu - r.cpu,
      memory - r.memory,
      scratch - r.scratch,
      gpuWithMultiplicities
        .filterNot(avail => r.gpuWithMultiplicities.contains(avail))
        .map(_._1),
      image
    )

  def substractAll = ResourceAvailable(
    cpu = 0,
    memory = 0,
    scratch = 0,
    gpu = Nil,
    image
  )

  def addBack(r: ResourceAllocated) =
    ResourceAvailable(
      cpu + r.cpu,
      memory + r.memory,
      scratch + r.scratch,
      gpu ++ r.gpu,
      image
    )

  def all =
    ResourceAllocated(
      cpu,
      memory,
      scratch,
      gpu,
      image
    )
  def minimum(r: ResourceRequest) =
    ResourceAllocated(
      r.cpu._1,
      r.memory,
      r.scratch,
      gpu.take(r.gpu),
      r.image
    )

  def maximum(r: ResourceRequest) = {
    val allocatedMemory = math.min(r.memory, memory)
    val allocatedCPU = math.min(cpu, r.cpu._2)
    val allocatedScratch = math.min(scratch, r.scratch)
    val allocatedGpu = gpu.take(r.gpu)
    ResourceAllocated(
      allocatedCPU,
      allocatedMemory,
      allocatedScratch,
      allocatedGpu,
      if (r.image.isEmpty || r.image == image) r.image else None
    )
  }

}

object ResourceAvailable {
  val empty = ResourceAvailable(0,0,0,Nil,None)
  private[tasks] def zipMultiplicities(l: List[Int]) =
    l.sorted.foldLeft(List.empty[(Int, Int)]) { case (acc, next) =>
      acc match {
        case (last, i) :: _ if last == next => (next, i + 1) :: acc
        case _                              => (next, 0) :: acc

      }
    }
  implicit val codec: JsonValueCodec[ResourceAvailable] = JsonCodecMaker.make
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
      scratch: Int,
      gpu: Int,
      image: Option[String]
  ): VersionedResourceRequest =
    VersionedResourceRequest(
      codeVersion,
      ResourceRequest((cpu, cpu), memory, scratch, gpu, image)
    )

  implicit val codec: JsonValueCodec[VersionedResourceRequest] =
    JsonCodecMaker.make
}

case class VersionedResourceAllocated(
    codeVersion: CodeVersion,
    cpuMemoryAllocated: ResourceAllocated
) {
  def cpu = cpuMemoryAllocated.cpu
  def memory = cpuMemoryAllocated.memory
  def scratch = cpuMemoryAllocated.scratch
  def gpu = cpuMemoryAllocated.gpu
}

object VersionedResourceAllocated {
  implicit val codec: JsonValueCodec[VersionedResourceAllocated] =
    JsonCodecMaker.make
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

}

object VersionedResourceAvailable {
  implicit val codec: JsonValueCodec[VersionedResourceAvailable] =
    JsonCodecMaker.make
}

case class RunningJobId(value: String)

object RunningJobId {
  implicit val codec: JsonValueCodec[RunningJobId] =
    JsonCodecMaker.make
}

case class PendingJobId(value: String)

object PendingJobId {
  implicit val codec: JsonValueCodec[PendingJobId] =
    JsonCodecMaker.make
}

case class Labels(values: List[(String, String)]) {
  def ++(other: Labels) = Labels(values ++ other.values)

}
object Labels {
  implicit val codec: JsonValueCodec[Labels] =
    JsonCodecMaker.make
  val empty = Labels(Nil)
}

case class LogRecord(
    data: String,
    timestamp: Instant
)

object LogRecord {
  implicit val codec: JsonValueCodec[LogRecord] =
    JsonCodecMaker.make
}
