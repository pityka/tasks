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

/** Predicate evaluated against the labels advertised by a worker
  * ([[ResourceAvailable.labels]]) to decide whether a task can be scheduled on
  * it.
  *
  * [[NodeSelector.Has]] expresses affinity, [[NodeSelector.Not]] expresses
  * avoidance, and [[NodeSelector.And]] / [[NodeSelector.Or]] compose them.
  * Labels are opaque strings; callers can encode key/value pairs with their own
  * convention (e.g. "region:us-east").
  */
sealed trait NodeSelector

object NodeSelector {

  /** No constraint — every worker matches. Default for [[ResourceRequest]]. */
  case object Always extends NodeSelector

  /** Worker must advertise `label`. */
  sealed trait Has extends NodeSelector { def label: String }
  object Has {
    private[tasks] final case class HasImpl(label: String) extends Has
    def apply(label: String): Has = HasImpl(label)
    def unapply(h: Has): Some[String] = Some(h.label)
  }

  sealed trait Not extends NodeSelector { def selector: NodeSelector }
  object Not {
    private[tasks] final case class NotImpl(selector: NodeSelector) extends Not
    def apply(selector: NodeSelector): Not = NotImpl(selector)
    def unapply(n: Not): Some[NodeSelector] = Some(n.selector)
  }

  sealed trait And extends NodeSelector { def selectors: List[NodeSelector] }
  object And {
    private[tasks] final case class AndImpl(selectors: List[NodeSelector])
        extends And
    def apply(selectors: List[NodeSelector]): And = AndImpl(selectors)
    def unapply(a: And): Some[List[NodeSelector]] = Some(a.selectors)
  }

  sealed trait Or extends NodeSelector { def selectors: List[NodeSelector] }
  object Or {
    private[tasks] final case class OrImpl(selectors: List[NodeSelector])
        extends Or
    def apply(selectors: List[NodeSelector]): Or = OrImpl(selectors)
    def unapply(o: Or): Some[List[NodeSelector]] = Some(o.selectors)
  }

  def matches(selector: NodeSelector, available: Set[String]): Boolean =
    selector match {
      case Always     => true
      case Has(label) => available.contains(label)
      case Not(s)     => !matches(s, available)
      case And(xs)    => xs.forall(matches(_, available))
      case Or(xs)     => xs.exists(matches(_, available))
    }

  private[shared] object view {
    sealed trait V
    case object Always extends V
    final case class Has(label: String) extends V
    final case class Not(selector: V) extends V
    final case class And(selectors: List[V]) extends V
    final case class Or(selectors: List[V]) extends V
  }

  private[shared] def toView(n: NodeSelector): view.V = n match {
    case NodeSelector.Always  => view.Always
    case NodeSelector.Has(l)  => view.Has(l)
    case NodeSelector.Not(s)  => view.Not(toView(s))
    case NodeSelector.And(xs) => view.And(xs.map(toView))
    case NodeSelector.Or(xs)  => view.Or(xs.map(toView))
  }

  private[shared] def fromView(v: view.V): NodeSelector = v match {
    case view.Always  => NodeSelector.Always
    case view.Has(l)  => NodeSelector.Has(l)
    case view.Not(s)  => NodeSelector.Not(fromView(s))
    case view.And(xs) => NodeSelector.And(xs.map(fromView))
    case view.Or(xs)  => NodeSelector.Or(xs.map(fromView))
  }

  implicit val codec: JsonValueCodec[NodeSelector] = {
    val innerCodec: JsonValueCodec[view.V] =
      JsonCodecMaker.make(CodecMakerConfig.withAllowRecursiveTypes(true))
    new JsonValueCodec[NodeSelector] {
      val nullValue: NodeSelector = null
      def encodeValue(x: NodeSelector, out: JsonWriter): Unit =
        innerCodec.encodeValue(toView(x), out)
      def decodeValue(in: JsonReader, default: NodeSelector): NodeSelector =
        fromView(innerCodec.decodeValue(in, null))
    }
  }
}

/** Reads the `name:value` labels advertised by a worker
  * ([[ResourceAvailable.labels]]). Only the first colon separates the name from
  * the value, so `build:2026:08` names `build` with value `2026:08`. A label
  * without a colon is a bare presence attribute and has no value.
  */
object Label {

  private val separator = ':'

  def name(label: String): String =
    label.indexOf(separator.toInt) match {
      case -1    => label
      case index => label.take(index)
    }

  def value(label: String): Option[String] =
    label.indexOf(separator.toInt) match {
      case -1    => None
      case index => Some(label.drop(index + 1))
    }

  def valueOf(labels: Set[String], attribute: String): Option[String] = {
    val values = labels.iterator
      .filter(label => name(label) == attribute)
      .flatMap(value)
      .toSet
    if (values.size == 1) Some(values.head) else None
  }
}

/** Asks for the same task to run on up to `maximum` workers at once.
  *
  * The caller receives the first success or the last failure.
  */
sealed trait Replication {
  def maximum: Int
  def placementAttribute: Option[String]
}

object Replication {
  private[tasks] final case class ReplicationImpl(
      maximum: Int,
      placementAttribute: Option[String]
  ) extends Replication {
    require(maximum >= 1, s"maximum must be positive, got $maximum")
  }

  def apply(maximum: Int, placementAttribute: Option[String]): Replication =
    ReplicationImpl(maximum, placementAttribute)

  def apply(maximum: Int): Replication = ReplicationImpl(maximum, None)

  def apply(maximum: Int, placementAttribute: String): Replication =
    ReplicationImpl(maximum, Some(placementAttribute))

  implicit val codec: JsonValueCodec[Replication] = {
    val innerCodec: JsonValueCodec[ReplicationImpl] = JsonCodecMaker.make
    new JsonValueCodec[Replication] {
      val nullValue: Replication = null
      def encodeValue(
          x: Replication,
          out: com.github.plokhotnyuk.jsoniter_scala.core.JsonWriter
      ): Unit =
        innerCodec.encodeValue(x.asInstanceOf[ReplicationImpl], out)
      def decodeValue(
          in: com.github.plokhotnyuk.jsoniter_scala.core.JsonReader,
          default: Replication
      ): Replication =
        innerCodec.decodeValue(in, null)
    }
  }
}

sealed trait ResourceRequest {
  def cpu: (Int, Int)
  def memory: Int
  def scratch: Int
  def gpu: Int
  def image: Option[String]
  def nodeSelector: Option[NodeSelector]
  def replication: Option[Replication]

  final def maximumCopies: Int = replication.fold(1)(_.maximum)

  final def placementAttribute: Option[String] =
    replication.filter(_.maximum > 1).flatMap(_.placementAttribute)
}

object ResourceRequest {

  private[tasks] final case class ResourceRequestImpl(
      cpu: (Int, Int),
      memory: Int,
      scratch: Int,
      gpu: Int,
      image: Option[String],
      nodeSelector: Option[NodeSelector],
      replication: Option[Replication]
  ) extends ResourceRequest

  implicit def toLogFeature(rm: ResourceRequest): scribe.LogFeature =
    scribe.data(
      Map(
        "resource-request-cpu" -> rm.cpu,
        "resource-request-memory" -> rm.memory,
        "resource-request-gpu" -> rm.gpu,
        "resource-request-scratch" -> rm.scratch,
        "resource-request-image" -> rm.image.getOrElse(
          "none"
        ),
        "resource-request-node-selector" -> rm.nodeSelector
          .fold("any")(_.toString),
        "resource-request-replication" -> rm.replication.fold("none")(r =>
          s"max=${r.maximum}${r.placementAttribute.fold("")(a => s"@$a")}"
        )
      )
    )

  def apply(
      cpu: (Int, Int),
      memory: Int,
      scratch: Int,
      gpu: Int,
      image: Option[String],
      nodeSelector: Option[NodeSelector],
      replication: Option[Replication]
  ): ResourceRequest =
    ResourceRequestImpl(
      cpu,
      memory,
      scratch,
      gpu,
      image,
      nodeSelector,
      replication
    )

  def apply(cpu: Int, memory: Int, scratch: Int, gpu: Int): ResourceRequest =
    ResourceRequestImpl((cpu, cpu), memory, scratch, gpu, None, None, None)

  def apply(
      cpu: (Int, Int),
      memory: Int,
      scratch: Int,
      gpu: Int,
      image: Option[String]
  ): ResourceRequest =
    ResourceRequestImpl(cpu, memory, scratch, gpu, image, None, None)

  def apply(
      cpu: (Int, Int),
      memory: Int,
      scratch: Int,
      gpu: Int,
      image: Option[String],
      nodeSelector: Option[NodeSelector]
  ): ResourceRequest =
    ResourceRequestImpl(cpu, memory, scratch, gpu, image, nodeSelector, None)

  implicit val codec: JsonValueCodec[ResourceRequest] = {
    val innerCodec: JsonValueCodec[ResourceRequestImpl] =
      JsonCodecMaker.make(CodecMakerConfig.withAllowRecursiveTypes(true))
    new JsonValueCodec[ResourceRequest] {
      val nullValue: ResourceRequest = null
      def encodeValue(
          x: ResourceRequest,
          out: com.github.plokhotnyuk.jsoniter_scala.core.JsonWriter
      ): Unit =
        innerCodec.encodeValue(x.asInstanceOf[ResourceRequestImpl], out)
      def decodeValue(
          in: com.github.plokhotnyuk.jsoniter_scala.core.JsonReader,
          default: ResourceRequest
      ): ResourceRequest =
        innerCodec.decodeValue(in, null)
    }
  }

}

sealed trait ResourceAllocated {
  def cpu: Int
  def memory: Int
  def scratch: Int
  def gpu: List[Int]
  def image: Option[String]
}

object ResourceAllocated {
  private[tasks] final case class ResourceAllocatedImpl(
      cpu: Int,
      memory: Int,
      scratch: Int,
      gpu: List[Int],
      image: Option[String]
  ) extends ResourceAllocated {
    require(
      gpu == gpu.distinct && gpu == gpu.sorted,
      s"gpu must be sorted and unique, got $gpu"
    )
  }

  def apply(
      cpu: Int,
      memory: Int,
      scratch: Int,
      gpu: List[Int],
      image: Option[String]
  ): ResourceAllocated =
    ResourceAllocatedImpl(cpu, memory, scratch, gpu, image)

  implicit val codec: JsonValueCodec[ResourceAllocated] = {
    val innerCodec: JsonValueCodec[ResourceAllocatedImpl] = JsonCodecMaker.make
    new JsonValueCodec[ResourceAllocated] {
      val nullValue: ResourceAllocated = null
      def encodeValue(
          x: ResourceAllocated,
          out: com.github.plokhotnyuk.jsoniter_scala.core.JsonWriter
      ): Unit =
        innerCodec.encodeValue(x.asInstanceOf[ResourceAllocatedImpl], out)
      def decodeValue(
          in: com.github.plokhotnyuk.jsoniter_scala.core.JsonReader,
          default: ResourceAllocated
      ): ResourceAllocated =
        innerCodec.decodeValue(in, null)
    }
  }
}

sealed trait ResourceAvailable {
  def cpu: Int
  def memory: Int
  def scratch: Int
  def gpu: List[Int]
  def image: Option[String]
  def labels: Set[String]

  final def numGpu = gpu.size

  final def canFulfillRequest(r: ResourceAllocated) =
    cpu >= r.cpu && memory >= r.memory && scratch >= r.scratch &&
      r.gpu.forall(gpu.contains) &&
      r.image.forall(reqImage => image.contains(reqImage))

  final def canFulfillRequest(r: ResourceRequest) =
    cpu >= r.cpu._1 && memory >= r.memory && scratch >= r.scratch && numGpu >= r.gpu && r.image
      .map(requestedImage => image.exists(_ == requestedImage))
      .getOrElse(true) &&
      r.nodeSelector.forall(NodeSelector.matches(_, labels))

  final def substract(r: ResourceRequest) = {
    val remainingCPU = math.max((cpu - r.cpu._2), 0)
    ResourceAvailable(
      remainingCPU,
      memory - r.memory,
      scratch - r.scratch,
      gpu = gpu.drop(r.gpu),
      image = image,
      labels = labels
    )
  }

  final def substract(r: ResourceAllocated) = {
    val rGpu = r.gpu.toSet
    ResourceAvailable(
      cpu - r.cpu,
      memory - r.memory,
      scratch - r.scratch,
      gpu.filterNot(rGpu.contains),
      image,
      labels
    )
  }

  final def substractAll = ResourceAvailable(
    cpu = 0,
    memory = 0,
    scratch = 0,
    gpu = Nil,
    image = image,
    labels = labels
  )

  final def addBack(r: ResourceAllocated) = {
    assert((gpu.toSet & r.gpu.toSet).isEmpty, s"$gpu + ${r.gpu}")
    ResourceAvailable(
      cpu = cpu + r.cpu,
      memory = memory + r.memory,
      scratch = scratch + r.scratch,
      gpu = (gpu ++ r.gpu).sorted,
      image = image,
      labels = labels
    )
  }

  final def all =
    ResourceAllocated(
      cpu,
      memory,
      scratch,
      gpu,
      image
    )
  final def minimum(r: ResourceRequest) =
    ResourceAllocated(
      r.cpu._1,
      r.memory,
      r.scratch,
      gpu.take(r.gpu),
      r.image
    )

  final def maximum(r: ResourceRequest) = {
    val allocatedMemory = math.min(r.memory, memory)
    val allocatedCPU = math.min(cpu, r.cpu._2)
    val allocatedScratch = math.min(scratch, r.scratch)
    val allocatedGpu = gpu.take(r.gpu)
    assert(allocatedMemory >= r.memory)
    assert(allocatedCPU >= r.cpu._1)
    assert(allocatedScratch >= r.scratch)
    assert(allocatedGpu.size >= r.gpu)
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
  private[tasks] final case class ResourceAvailableImpl(
      cpu: Int,
      memory: Int,
      scratch: Int,
      gpu: List[Int],
      image: Option[String],
      labels: Set[String]
  ) extends ResourceAvailable {
    require(
      gpu == gpu.distinct && gpu == gpu.sorted,
      s"gpu must be sorted and unique, got $gpu"
    )
  }

  def apply(
      cpu: Int,
      memory: Int,
      scratch: Int,
      gpu: List[Int],
      image: Option[String],
      labels: Set[String]
  ): ResourceAvailable =
    ResourceAvailableImpl(cpu, memory, scratch, gpu, image, labels)

  def apply(
      cpu: Int,
      memory: Int,
      scratch: Int,
      gpu: List[Int],
      image: Option[String]
  ): ResourceAvailable =
    ResourceAvailableImpl(cpu, memory, scratch, gpu, image, Set.empty)

  implicit def toLogFeature(rm: ResourceAvailable): scribe.LogFeature =
    scribe.data(
      Map(
        "available-resource-cpu" -> rm.cpu,
        "available-resource-memory" -> rm.memory,
        "available-resource-gpu" -> rm.gpu.mkString(","),
        "available-resource-scratch" -> rm.scratch,
        "available-resource-image" -> rm.image.getOrElse("none"),
        "available-resource-labels" -> rm.labels.toList.sorted.mkString(",")
      )
    )
  val empty = ResourceAvailable(0, 0, 0, Nil, None)
  implicit val codec: JsonValueCodec[ResourceAvailable] = {
    val innerCodec: JsonValueCodec[ResourceAvailableImpl] = JsonCodecMaker.make
    new JsonValueCodec[ResourceAvailable] {
      val nullValue: ResourceAvailable = null
      def encodeValue(
          x: ResourceAvailable,
          out: com.github.plokhotnyuk.jsoniter_scala.core.JsonWriter
      ): Unit =
        innerCodec.encodeValue(x.asInstanceOf[ResourceAvailableImpl], out)
      def decodeValue(
          in: com.github.plokhotnyuk.jsoniter_scala.core.JsonReader,
          default: ResourceAvailable
      ): ResourceAvailable =
        innerCodec.decodeValue(in, null)
    }
  }
}

sealed trait VersionedResourceRequest {
  def codeVersion: CodeVersion
  def cpuMemoryRequest: ResourceRequest
  final def cpu = cpuMemoryRequest.cpu
  final def memory = cpuMemoryRequest.memory
  final def scratch = cpuMemoryRequest.scratch
}

object VersionedResourceRequest {
  private[tasks] final case class VersionedResourceRequestImpl(
      codeVersion: CodeVersion,
      cpuMemoryRequest: ResourceRequest
  ) extends VersionedResourceRequest

  def apply(
      codeVersion: CodeVersion,
      cpuMemoryRequest: ResourceRequest
  ): VersionedResourceRequest =
    VersionedResourceRequestImpl(codeVersion, cpuMemoryRequest)

  def apply(
      codeVersion: CodeVersion,
      cpu: Int,
      memory: Int,
      scratch: Int,
      gpu: Int,
      image: Option[String]
  ): VersionedResourceRequest =
    VersionedResourceRequestImpl(
      codeVersion,
      ResourceRequest((cpu, cpu), memory, scratch, gpu, image)
    )

  implicit val codec: JsonValueCodec[VersionedResourceRequest] = {
    val innerCodec: JsonValueCodec[VersionedResourceRequestImpl] =
      JsonCodecMaker.make
    new JsonValueCodec[VersionedResourceRequest] {
      val nullValue: VersionedResourceRequest = null
      def encodeValue(
          x: VersionedResourceRequest,
          out: com.github.plokhotnyuk.jsoniter_scala.core.JsonWriter
      ): Unit =
        innerCodec.encodeValue(
          x.asInstanceOf[VersionedResourceRequestImpl],
          out
        )
      def decodeValue(
          in: com.github.plokhotnyuk.jsoniter_scala.core.JsonReader,
          default: VersionedResourceRequest
      ): VersionedResourceRequest =
        innerCodec.decodeValue(in, null)
    }
  }
}

sealed trait VersionedResourceAllocated {
  def codeVersion: CodeVersion
  def cpuMemoryAllocated: ResourceAllocated
  final def cpu = cpuMemoryAllocated.cpu
  final def memory = cpuMemoryAllocated.memory
  final def scratch = cpuMemoryAllocated.scratch
  final def gpu = cpuMemoryAllocated.gpu
}

object VersionedResourceAllocated {
  private[tasks] final case class VersionedResourceAllocatedImpl(
      codeVersion: CodeVersion,
      cpuMemoryAllocated: ResourceAllocated
  ) extends VersionedResourceAllocated

  def apply(
      codeVersion: CodeVersion,
      cpuMemoryAllocated: ResourceAllocated
  ): VersionedResourceAllocated =
    VersionedResourceAllocatedImpl(codeVersion, cpuMemoryAllocated)

  implicit def toLogFeature(rm: VersionedResourceAllocated): scribe.LogFeature =
    scribe.data(
      Map(
        "allocated-resource-cpu" -> rm.cpuMemoryAllocated.cpu,
        "allocated-resource-memory" -> rm.cpuMemoryAllocated.memory,
        "allocated-resource-gpu" -> rm.cpuMemoryAllocated.gpu.mkString(","),
        "allocated-resource-scratch" -> rm.cpuMemoryAllocated.scratch,
        "allocated-resource-image" -> rm.cpuMemoryAllocated.image.getOrElse(
          "none"
        )
      )
    )
  implicit val codec: JsonValueCodec[VersionedResourceAllocated] = {
    val innerCodec: JsonValueCodec[VersionedResourceAllocatedImpl] =
      JsonCodecMaker.make
    new JsonValueCodec[VersionedResourceAllocated] {
      val nullValue: VersionedResourceAllocated = null
      def encodeValue(
          x: VersionedResourceAllocated,
          out: com.github.plokhotnyuk.jsoniter_scala.core.JsonWriter
      ): Unit =
        innerCodec.encodeValue(
          x.asInstanceOf[VersionedResourceAllocatedImpl],
          out
        )
      def decodeValue(
          in: com.github.plokhotnyuk.jsoniter_scala.core.JsonReader,
          default: VersionedResourceAllocated
      ): VersionedResourceAllocated =
        innerCodec.decodeValue(in, null)
    }
  }
}

sealed trait VersionedResourceAvailable {
  def codeVersion: CodeVersion
  def cpuMemoryAvailable: ResourceAvailable

  final def canFulfillRequest(r: VersionedResourceRequest) =
    r.codeVersion == codeVersion && cpuMemoryAvailable.canFulfillRequest(
      r.cpuMemoryRequest
    )

  final def substract(r: VersionedResourceRequest) =
    VersionedResourceAvailable(
      codeVersion,
      cpuMemoryAvailable.substract(r.cpuMemoryRequest)
    )

  final def substract(r: VersionedResourceAllocated) =
    VersionedResourceAvailable(
      codeVersion,
      cpuMemoryAvailable.substract(r.cpuMemoryAllocated)
    )

  final def addBack(r: VersionedResourceAllocated) =
    VersionedResourceAvailable(
      codeVersion,
      cpuMemoryAvailable.addBack(r.cpuMemoryAllocated)
    )

  final def maximum(r: VersionedResourceRequest) =
    VersionedResourceAllocated(
      codeVersion,
      cpuMemoryAvailable.maximum(r.cpuMemoryRequest)
    )

}

object VersionedResourceAvailable {
  private[tasks] final case class VersionedResourceAvailableImpl(
      codeVersion: CodeVersion,
      cpuMemoryAvailable: ResourceAvailable
  ) extends VersionedResourceAvailable

  def apply(
      codeVersion: CodeVersion,
      cpuMemoryAvailable: ResourceAvailable
  ): VersionedResourceAvailable =
    VersionedResourceAvailableImpl(codeVersion, cpuMemoryAvailable)

  implicit def toLogFeature(rm: VersionedResourceAvailable): scribe.LogFeature =
    scribe.data(
      Map(
        "available-resource-cpu" -> rm.cpuMemoryAvailable.cpu,
        "available-resource-memory" -> rm.cpuMemoryAvailable.memory,
        "available-resource-gpu" -> rm.cpuMemoryAvailable.gpu.mkString(","),
        "available-resource-scratch" -> rm.cpuMemoryAvailable.scratch,
        "available-resource-image" -> rm.cpuMemoryAvailable.image.getOrElse(
          "none"
        ),
        "available-resource-labels" -> rm.cpuMemoryAvailable.labels.toList.sorted
          .mkString(",")
      )
    )
  implicit val codec: JsonValueCodec[VersionedResourceAvailable] = {
    val innerCodec: JsonValueCodec[VersionedResourceAvailableImpl] =
      JsonCodecMaker.make
    new JsonValueCodec[VersionedResourceAvailable] {
      val nullValue: VersionedResourceAvailable = null
      def encodeValue(
          x: VersionedResourceAvailable,
          out: com.github.plokhotnyuk.jsoniter_scala.core.JsonWriter
      ): Unit =
        innerCodec.encodeValue(
          x.asInstanceOf[VersionedResourceAvailableImpl],
          out
        )
      def decodeValue(
          in: com.github.plokhotnyuk.jsoniter_scala.core.JsonReader,
          default: VersionedResourceAvailable
      ): VersionedResourceAvailable =
        innerCodec.decodeValue(in, null)
    }
  }
}

sealed trait RunningJobId { def value: String }
object RunningJobId {
  private[tasks] final case class RunningJobIdImpl(value: String)
      extends RunningJobId
  def apply(value: String): RunningJobId = RunningJobIdImpl(value)
  def unapply(r: RunningJobId): Some[String] = Some(r.value)
  implicit def toLogFeature(rm: RunningJobId): scribe.LogFeature = scribe.data(
    Map("running-job-id" -> rm.value)
  )
  implicit val codec: JsonValueCodec[RunningJobId] = {
    val innerCodec: JsonValueCodec[RunningJobIdImpl] = JsonCodecMaker.make
    new JsonValueCodec[RunningJobId] {
      val nullValue: RunningJobId = null
      def encodeValue(x: RunningJobId, out: JsonWriter): Unit =
        innerCodec.encodeValue(x.asInstanceOf[RunningJobIdImpl], out)
      def decodeValue(in: JsonReader, default: RunningJobId): RunningJobId =
        innerCodec.decodeValue(in, null)
    }
  }
}

sealed trait PendingJobId { def value: String }
object PendingJobId {
  private[tasks] final case class PendingJobIdImpl(value: String)
      extends PendingJobId
  def apply(value: String): PendingJobId = PendingJobIdImpl(value)
  def unapply(p: PendingJobId): Some[String] = Some(p.value)
  implicit def toLogFeature(rm: PendingJobId): scribe.LogFeature = scribe.data(
    Map("pending-job-id" -> rm.value)
  )
  implicit val codec: JsonValueCodec[PendingJobId] = {
    val innerCodec: JsonValueCodec[PendingJobIdImpl] = JsonCodecMaker.make
    new JsonValueCodec[PendingJobId] {
      val nullValue: PendingJobId = null
      def encodeValue(x: PendingJobId, out: JsonWriter): Unit =
        innerCodec.encodeValue(x.asInstanceOf[PendingJobIdImpl], out)
      def decodeValue(in: JsonReader, default: PendingJobId): PendingJobId =
        innerCodec.decodeValue(in, null)
    }
  }
}

sealed trait Labels {
  def values: List[(String, String)]
  final def ++(other: Labels): Labels = Labels(values ++ other.values)
}
object Labels {
  private[tasks] final case class LabelsImpl(values: List[(String, String)])
      extends Labels
  def apply(values: List[(String, String)]): Labels = LabelsImpl(values)
  def unapply(l: Labels): Some[List[(String, String)]] = Some(l.values)
  val empty: Labels = LabelsImpl(Nil)
  implicit val codec: JsonValueCodec[Labels] = {
    val innerCodec: JsonValueCodec[LabelsImpl] = JsonCodecMaker.make
    new JsonValueCodec[Labels] {
      val nullValue: Labels = null
      def encodeValue(x: Labels, out: JsonWriter): Unit =
        innerCodec.encodeValue(x.asInstanceOf[LabelsImpl], out)
      def decodeValue(in: JsonReader, default: Labels): Labels =
        innerCodec.decodeValue(in, null)
    }
  }
}

sealed trait LogRecord {
  def data: String
  def timestamp: Instant
}
object LogRecord {
  private[tasks] final case class LogRecordImpl(
      data: String,
      timestamp: Instant
  ) extends LogRecord
  def apply(data: String, timestamp: Instant): LogRecord =
    LogRecordImpl(data, timestamp)
  implicit val codec: JsonValueCodec[LogRecord] = {
    val innerCodec: JsonValueCodec[LogRecordImpl] = JsonCodecMaker.make
    new JsonValueCodec[LogRecord] {
      val nullValue: LogRecord = null
      def encodeValue(x: LogRecord, out: JsonWriter): Unit =
        innerCodec.encodeValue(x.asInstanceOf[LogRecordImpl], out)
      def decodeValue(in: JsonReader, default: LogRecord): LogRecord =
        innerCodec.decodeValue(in, null)
    }
  }
}
