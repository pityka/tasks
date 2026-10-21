package tasks.queue

import scala.jdk.CollectionConverters._

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model._

import tasks.elastic.NodeRegistryState
import tasks.queue.DynamoCodecs._
import tasks.queue.QueueImpl._
import tasks.util.message.Address
import tasks.util.message.LauncherName
import tasks.util.message.MessageData.ScheduleTask
import tasks.util.message.Node

private[tasks] object DynamoDiff {

  import DynamoQueueState.Attr
  import DynamoQueueState.StatusQueued
  import DynamoQueueState.StatusScheduled
  import DynamoQueueState.TasksPartitionKey
  import DynamoQueueState.ResultsPartitionKey
  import DynamoQueueState.ResourcesPartitionKey
  import DynamoQueueState.ResourcesSortKey
  import DynamoQueueState.MaxOpsPerTransaction
  import DynamoQueueState.MaxDataBytesPerTransaction
  import DynamoQueueState.MaxDataOpsPerChunk
  import DynamoQueueState.ChunkPayloadBytes
  import DynamoQueueState.ChunkIndexWidth
  import DynamoQueueState.ChunkSeparator

  case class TaskItemFields(
      sch: ScheduleTask,
      status: String,
      proxies: List[Proxy],
      dispatches: List[Dispatch],
      placementValue: Option[String],
      resultDelivered: Boolean
  )

  case class ResourcesFields(
      knownLaunchers: Map[LauncherName, Option[Node]],
      counters: Map[LauncherName, Long],
      nodes: NodeRegistryState.State,
      mainProcesses: Set[String]
  )

  case class Plan(
      chunks: List[List[TransactWriteItem]]
  )

  private def s(v: String): AttributeValue =
    AttributeValue.builder().s(v).build()

  private def n(v: Long): AttributeValue =
    AttributeValue.builder().n(v.toString).build()

  private def b(bytes: Array[Byte]): AttributeValue =
    AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build()

  private def bool(v: Boolean): AttributeValue =
    AttributeValue.builder().bool(java.lang.Boolean.valueOf(v)).build()

  private def l(items: List[AttributeValue]): AttributeValue =
    AttributeValue.builder().l(items.asJava).build()

  private def m(entries: Map[String, AttributeValue]): AttributeValue =
    AttributeValue.builder().m(entries.asJava).build()

  def encodeTaskAttrs(
      sk: String,
      item: TaskItemFields,
      ver: Long
  ): Map[String, AttributeValue] = {
    val proxiesList = item.proxies.map(p => b(encode(p)))
    val dispatchesList = item.dispatches.map(d => b(encode(d)))
    val base = Map[String, AttributeValue](
      Attr.Pk -> s(TasksPartitionKey),
      Attr.Sk -> s(sk),
      Attr.Status -> s(item.status),
      Attr.Sch -> b(encode(item.sch)),
      Attr.Proxies -> l(proxiesList),
      Attr.Dispatches -> l(dispatchesList),
      Attr.ResultDelivered -> bool(item.resultDelivered),
      Attr.Ver -> n(ver)
    )
    item.placementValue match {
      case None     => base
      case Some(pv) => base + (Attr.PlacementValue -> s(pv))
    }
  }

  def decodeTaskItem(
      item: java.util.Map[String, AttributeValue]
  ): (String, (ScheduleTask, TaskItemFields, Long)) = {
    val attrs = item.asScala
    val sk = attrs(Attr.Sk).s()
    val status = attrs(Attr.Status).s()
    val sch = decode[ScheduleTask](attrs(Attr.Sch).b().asByteArray())
    val proxies = attrs(Attr.Proxies)
      .l()
      .asScala
      .toList
      .map(av => decode[Proxy](av.b().asByteArray()))
    val dispatches = attrs(Attr.Dispatches)
      .l()
      .asScala
      .toList
      .map(av => decode[Dispatch](av.b().asByteArray()))
    val placementValue = attrs.get(Attr.PlacementValue).map(_.s())
    val resultDelivered = attrs(Attr.ResultDelivered).bool().booleanValue()
    val ver = attrs(Attr.Ver).n().toLong
    val fields = TaskItemFields(
      sch = sch,
      status = status,
      proxies = proxies,
      dispatches = dispatches,
      placementValue = placementValue,
      resultDelivered = resultDelivered
    )
    (sk, (sch, fields, ver))
  }

  def encodeResources(
      knownLaunchers: Map[LauncherName, Option[Node]],
      counters: Map[LauncherName, Long],
      nodes: NodeRegistryState.State,
      mainProcesses: Set[String],
      ver: Long
  ): Map[String, AttributeValue] = {
    val knownLaunchersMap: Map[String, AttributeValue] =
      knownLaunchers.map { case (name, nodeOpt) =>
        name.name -> b(encode(nodeOpt))
      }
    val countersMap: Map[String, AttributeValue] =
      counters.map { case (name, value) => name.name -> n(value) }
    val mainProcList: List[AttributeValue] =
      mainProcesses.toList.sorted.map(s)
    Map(
      Attr.Pk -> s(ResourcesPartitionKey),
      Attr.Sk -> s(ResourcesSortKey),
      Attr.KnownLaunchers -> m(knownLaunchersMap),
      Attr.Counters -> m(countersMap),
      Attr.Nodes -> b(encode(nodes)),
      Attr.MainProcesses -> l(mainProcList),
      Attr.Ver -> n(ver)
    )
  }

  def decodeResources(
      item: java.util.Map[String, AttributeValue]
  ): (ResourcesFields, Long) = {
    val attrs = item.asScala
    val knownLaunchers: Map[LauncherName, Option[Node]] =
      attrs(Attr.KnownLaunchers)
        .m()
        .asScala
        .toMap
        .map { case (name, av) =>
          LauncherName(name) -> decode[Option[Node]](av.b().asByteArray())
        }
    val counters: Map[LauncherName, Long] =
      attrs(Attr.Counters)
        .m()
        .asScala
        .toMap
        .map { case (name, av) =>
          LauncherName(name) -> av.n().toLong
        }
    val nodes: NodeRegistryState.State =
      decode[NodeRegistryState.State](attrs(Attr.Nodes).b().asByteArray())
    val mainProcesses: Set[String] =
      attrs(Attr.MainProcesses).l().asScala.toList.map(_.s()).toSet
    val ver = attrs(Attr.Ver).n().toLong
    (
      ResourcesFields(
        knownLaunchers = knownLaunchers,
        counters = counters,
        nodes = nodes,
        mainProcesses = mainProcesses
      ),
      ver
    )
  }

  private val chunkIndexFormat: String = "%0" + ChunkIndexWidth + "d"

  private[tasks] def chunkSk(addressValue: String, index: Int): String =
    addressValue + ChunkSeparator + chunkIndexFormat.format(index)

  private[tasks] def parseAddressFromSk(sk: String): String =
    sk.substring(0, sk.lastIndexOf(ChunkSeparator))

  private[tasks] def splitIntoChunks(bytes: Array[Byte]): List[Array[Byte]] =
    if (bytes.length <= ChunkPayloadBytes) List(bytes)
    else bytes.grouped(ChunkPayloadBytes).toList

  private[tasks] def completedResultChunks(
      completedResults: Map[Address, ProxyResult]
  ): Map[String, Array[Byte]] =
    completedResults.flatMap { case (address, result) =>
      val serialized = encode(result)
      splitIntoChunks(serialized).zipWithIndex.map { case (chunk, i) =>
        chunkSk(address.value, i) -> chunk
      }
    }

  def encodeResultChunkAttrs(
      sk: String,
      chunk: Array[Byte],
      ver: Long
  ): Map[String, AttributeValue] = Map(
    Attr.Pk -> s(ResultsPartitionKey),
    Attr.Sk -> s(sk),
    Attr.Blob -> b(chunk),
    Attr.Ver -> n(ver)
  )

  def decodeResultChunkItem(
      item: java.util.Map[String, AttributeValue]
  ): (String, (Array[Byte], Long)) = {
    val attrs = item.asScala
    val sk = attrs(Attr.Sk).s()
    val chunk = attrs(Attr.Blob).b().asByteArray()
    val ver = attrs(Attr.Ver).n().toLong
    (sk, (chunk, ver))
  }

  def assembleCompletedResults(
      chunkMap: Map[String, (Array[Byte], Long)]
  ): Map[Address, ProxyResult] = {
    val byAddress =
      chunkMap.toList.groupBy { case (sk, _) => parseAddressFromSk(sk) }
    byAddress.map { case (addressValue, entries) =>
      val sorted = entries.sortBy(_._1)
      val fullBytes =
        sorted.flatMap { case (_, (bytes, _)) => bytes }.toArray
      Address(addressValue) -> decode[ProxyResult](fullBytes)
    }
  }

  def assembleState(
      taskMap: Map[String, (ScheduleTask, TaskItemFields, Long)],
      completedResults: Map[Address, ProxyResult],
      resources: ResourcesFields
  ): State = {
    val queuedTasks = taskMap.values
      .filter(_._2.status == StatusQueued)
      .map { case (sch, fields, _) =>
        QueueImpl.project(sch) -> ((sch, fields.proxies))
      }
      .toMap
    val scheduledTasks = taskMap.values
      .filter(_._2.status == StatusScheduled)
      .map { case (sch, fields, _) =>
        QueueImpl.project(sch) -> ScheduledTask(
          sch = sch,
          dispatches = fields.dispatches,
          proxies = fields.proxies,
          placementValue = fields.placementValue,
          resultDelivered = fields.resultDelivered
        )
      }
      .toMap
    State(
      queuedTasks = queuedTasks,
      scheduledTasks = scheduledTasks,
      knownLaunchers = resources.knownLaunchers,
      counters = resources.counters,
      nodes = resources.nodes,
      completedResults = completedResults,
      mainProcesses = resources.mainProcesses
    )
  }

  private def taskItemsOf(
      state: State
  ): Map[String, TaskItemFields] = {
    val queued = state.queuedTasks.map { case (projection, (sch, proxies)) =>
      projection.description.hash -> TaskItemFields(
        sch = sch,
        status = StatusQueued,
        proxies = proxies,
        dispatches = Nil,
        placementValue = None,
        resultDelivered = false
      )
    }
    val scheduled = state.scheduledTasks.map {
      case (projection, scheduledTask) =>
        projection.description.hash -> TaskItemFields(
          sch = scheduledTask.sch,
          status = StatusScheduled,
          proxies = scheduledTask.proxies,
          dispatches = scheduledTask.dispatches,
          placementValue = scheduledTask.placementValue,
          resultDelivered = scheduledTask.resultDelivered
        )
    }
    queued ++ scheduled
  }

  private def resourcesOf(state: State): ResourcesFields =
    ResourcesFields(
      knownLaunchers = state.knownLaunchers,
      counters = state.counters,
      nodes = state.nodes,
      mainProcesses = state.mainProcesses
    )

  private def resourcesEqual(a: ResourcesFields, b: ResourcesFields): Boolean =
    a == b

  private def putNewTaskItem(
      table: String,
      sk: String,
      item: TaskItemFields
  ): TransactWriteItem = {
    val put = Put
      .builder()
      .tableName(table)
      .item(encodeTaskAttrs(sk, item, 1L).asJava)
      .conditionExpression("attribute_not_exists(#pk)")
      .expressionAttributeNames(Map("#pk" -> Attr.Pk).asJava)
      .build()
    TransactWriteItem.builder().put(put).build()
  }

  private def updateTaskItem(
      table: String,
      sk: String,
      item: TaskItemFields,
      baseVer: Long
  ): TransactWriteItem = {
    val put = Put
      .builder()
      .tableName(table)
      .item(encodeTaskAttrs(sk, item, baseVer + 1).asJava)
      .conditionExpression("#ver = :ver")
      .expressionAttributeNames(Map("#ver" -> Attr.Ver).asJava)
      .expressionAttributeValues(Map(":ver" -> n(baseVer)).asJava)
      .build()
    TransactWriteItem.builder().put(put).build()
  }

  private def deleteTaskItem(
      table: String,
      sk: String,
      baseVer: Long
  ): TransactWriteItem = {
    val del = Delete
      .builder()
      .tableName(table)
      .key(
        Map(
          Attr.Pk -> s(TasksPartitionKey),
          Attr.Sk -> s(sk)
        ).asJava
      )
      .conditionExpression("#ver = :ver")
      .expressionAttributeNames(Map("#ver" -> Attr.Ver).asJava)
      .expressionAttributeValues(Map(":ver" -> n(baseVer)).asJava)
      .build()
    TransactWriteItem.builder().delete(del).build()
  }

  private def putResources(
      table: String,
      resources: ResourcesFields,
      newVer: Long,
      baseVer: Long
  ): TransactWriteItem = {
    val attrs = encodeResources(
      knownLaunchers = resources.knownLaunchers,
      counters = resources.counters,
      nodes = resources.nodes,
      mainProcesses = resources.mainProcesses,
      ver = newVer
    )
    val put = Put
      .builder()
      .tableName(table)
      .item(attrs.asJava)
      .conditionExpression("#ver = :ver")
      .expressionAttributeNames(Map("#ver" -> Attr.Ver).asJava)
      .expressionAttributeValues(Map(":ver" -> n(baseVer)).asJava)
      .build()
    TransactWriteItem.builder().put(put).build()
  }

  private def putNewResultChunk(
      table: String,
      sk: String,
      chunk: Array[Byte]
  ): TransactWriteItem = {
    val put = Put
      .builder()
      .tableName(table)
      .item(encodeResultChunkAttrs(sk, chunk, 1L).asJava)
      .conditionExpression("attribute_not_exists(#pk)")
      .expressionAttributeNames(Map("#pk" -> Attr.Pk).asJava)
      .build()
    TransactWriteItem.builder().put(put).build()
  }

  private def updateResultChunk(
      table: String,
      sk: String,
      chunk: Array[Byte],
      baseVer: Long
  ): TransactWriteItem = {
    val put = Put
      .builder()
      .tableName(table)
      .item(encodeResultChunkAttrs(sk, chunk, baseVer + 1).asJava)
      .conditionExpression("#ver = :ver")
      .expressionAttributeNames(Map("#ver" -> Attr.Ver).asJava)
      .expressionAttributeValues(Map(":ver" -> n(baseVer)).asJava)
      .build()
    TransactWriteItem.builder().put(put).build()
  }

  private def deleteResultChunk(
      table: String,
      sk: String,
      baseVer: Long
  ): TransactWriteItem = {
    val del = Delete
      .builder()
      .tableName(table)
      .key(
        Map(
          Attr.Pk -> s(ResultsPartitionKey),
          Attr.Sk -> s(sk)
        ).asJava
      )
      .conditionExpression("#ver = :ver")
      .expressionAttributeNames(Map("#ver" -> Attr.Ver).asJava)
      .expressionAttributeValues(Map(":ver" -> n(baseVer)).asJava)
      .build()
    TransactWriteItem.builder().delete(del).build()
  }

  private def resourcesConditionCheck(
      table: String,
      baseVer: Long
  ): TransactWriteItem = {
    val cc = ConditionCheck
      .builder()
      .tableName(table)
      .key(
        Map(
          Attr.Pk -> s(ResourcesPartitionKey),
          Attr.Sk -> s(ResourcesSortKey)
        ).asJava
      )
      .conditionExpression("#ver = :ver")
      .expressionAttributeNames(Map("#ver" -> Attr.Ver).asJava)
      .expressionAttributeValues(Map(":ver" -> n(baseVer)).asJava)
      .build()
    TransactWriteItem.builder().conditionCheck(cc).build()
  }

  def plan(
      table: String,
      oldState: State,
      newState: State,
      baseTaskVersions: Map[String, Long],
      baseResultChunkVersions: Map[String, Long],
      baseResourcesVersion: Long
  ): Plan = {
    val oldItems = taskItemsOf(oldState)
    val newItems = taskItemsOf(newState)
    val allTaskKeys = oldItems.keySet ++ newItems.keySet

    val taskOps: List[TransactWriteItem] = allTaskKeys.toList.sorted.flatMap { sk =>
      (oldItems.get(sk), newItems.get(sk)) match {
        case (Some(_), None) =>
          val baseVer = baseTaskVersions.getOrElse(sk, 0L)
          Some(deleteTaskItem(table, sk, baseVer))
        case (None, Some(item)) =>
          Some(putNewTaskItem(table, sk, item))
        case (Some(oldItem), Some(newItem)) if oldItem == newItem => None
        case (Some(_), Some(newItem)) =>
          val baseVer = baseTaskVersions.getOrElse(sk, 0L)
          Some(updateTaskItem(table, sk, newItem, baseVer))
        case (None, None) => None
      }
    }

    val oldResultChunks = completedResultChunks(oldState.completedResults)
    val newResultChunks = completedResultChunks(newState.completedResults)
    val allResultKeys = oldResultChunks.keySet ++ newResultChunks.keySet

    val resultOps: List[TransactWriteItem] =
      allResultKeys.toList.sorted.flatMap { sk =>
        (oldResultChunks.get(sk), newResultChunks.get(sk)) match {
          case (Some(_), None) =>
            val baseVer = baseResultChunkVersions.getOrElse(sk, 0L)
            Some(deleteResultChunk(table, sk, baseVer))
          case (None, Some(chunk)) =>
            Some(putNewResultChunk(table, sk, chunk))
          case (Some(oldChunk), Some(newChunk))
              if java.util.Arrays.equals(oldChunk, newChunk) =>
            None
          case (Some(_), Some(chunk)) =>
            val baseVer = baseResultChunkVersions.getOrElse(sk, 0L)
            Some(updateResultChunk(table, sk, chunk, baseVer))
          case (None, None) => None
        }
      }

    val oldResources = resourcesOf(oldState)
    val newResources = resourcesOf(newState)
    val resourcesChanged = !resourcesEqual(oldResources, newResources)

    val resourcesOp: Option[TransactWriteItem] =
      if (resourcesChanged)
        Some(
          putResources(
            table,
            newResources,
            baseResourcesVersion + 1,
            baseResourcesVersion
          )
        )
      else None

    val taskUnits: List[(List[TransactWriteItem], Int)] =
      taskOps.map(op => List(op) -> estimateOpBytes(op))

    val resultUnits: List[(List[TransactWriteItem], Int)] = {
      val grouped = resultOps.groupBy(opAddress)
      grouped.toList.map { case (addressValue, ops) =>
        val bytes = ops.map(estimateOpBytes).sum
        if (bytes > MaxDataBytesPerTransaction) {
          val address = Address(addressValue)
          val resultOpt = newState.completedResults
            .get(address)
            .orElse(oldState.completedResults.get(address))
          val fromScheduled = oldState.scheduledTasks.values
            .find(_.proxies.exists(_.address == address))
            .map(_.sch.description)
          val fromQueued = oldState.queuedTasks.values.collectFirst {
            case (sch, proxies) if proxies.exists(_.address == address) =>
              sch.description
          }
          val taskHint = fromScheduled.orElse(fromQueued)
          throw new DynamoQueueState.OversizedUnitException(
            formatOversizedMessage(
              addressValue = addressValue,
              bytes = bytes,
              limit = MaxDataBytesPerTransaction,
              result = resultOpt,
              taskDescription = taskHint
            )
          )
        }
        ops -> bytes
      }
    }

    val units: List[(List[TransactWriteItem], Int)] = taskUnits ++ resultUnits

    if (units.isEmpty && resourcesOp.isEmpty) Plan(Nil)
    else {
      val dataChunks: List[List[TransactWriteItem]] = packUnits(units)
      val barrier = resourcesConditionCheck(table, baseResourcesVersion)

      val middleChunks: List[List[TransactWriteItem]] =
        (resourcesOp, dataChunks) match {
          case (_, Nil) => Nil
          case (None, chunks) =>
            chunks.map(chunk => chunk :+ barrier)
          case (Some(resPut), chunks) =>
            val init = chunks.init
            val last = chunks.last
            init.map(chunk => chunk :+ barrier) :+ (last :+ resPut)
        }

      val finalChunks: List[List[TransactWriteItem]] =
        if (middleChunks.isEmpty && resourcesOp.isDefined)
          List(List(resourcesOp.get))
        else middleChunks

      Plan(finalChunks)
    }
  }

  private def packUnits(
      units: List[(List[TransactWriteItem], Int)]
  ): List[List[TransactWriteItem]] = {
    val builder = scala.collection.mutable.ListBuffer.empty[List[TransactWriteItem]]
    var current = List.empty[TransactWriteItem]
    var currentBytes = 0
    units.foreach { case (unitOps, unitBytes) =>
      val newSize = current.size + unitOps.size
      val newBytes = currentBytes + unitBytes
      if (
        current.nonEmpty &&
        (newSize > MaxDataOpsPerChunk || newBytes > MaxDataBytesPerTransaction)
      ) {
        builder += current
        current = unitOps
        currentBytes = unitBytes
      } else {
        current = current ++ unitOps
        currentBytes = newBytes
      }
    }
    if (current.nonEmpty) builder += current
    builder.toList
  }

  private def opAddress(op: TransactWriteItem): String = {
    val sk =
      if (op.put() != null) op.put().item().get(Attr.Sk).s()
      else if (op.delete() != null) op.delete().key().get(Attr.Sk).s()
      else throw new IllegalArgumentException("result op must be Put or Delete")
    parseAddressFromSk(sk)
  }

  private val OversizedPreviewChars: Int = 512

  private def truncate(s: String, max: Int): String =
    if (s.length <= max) s
    else s.substring(0, max) + s"… [truncated, total ${s.length} chars]"

  private def describeResult(result: ProxyResult): String = result match {
    case QueueImpl.ProxyResultSuccess(untyped, retrievedFromCache) =>
      val inlineBase64 = untyped.data.value
      val filesCount = untyped.files.size
      val mutableFilesCount = untyped.mutableFiles.fold(0)(_.size)
      s"ProxyResultSuccess(" +
        s"files=$filesCount, mutableFiles=$mutableFilesCount, " +
        s"retrievedFromCache=$retrievedFromCache, " +
        s"inlineBase64Length=${inlineBase64.length}, " +
        s"inlineBase64Prefix='${truncate(inlineBase64, OversizedPreviewChars)}'" +
        ")"
    case QueueImpl.ProxyResultFailure(cause) =>
      val stack = cause.getStackTrace
        .take(5)
        .map(e => s"    at ${e.getClassName}.${e.getMethodName}(${e.getFileName}:${e.getLineNumber})")
        .mkString("\n")
      val msg = Option(cause.getMessage).getOrElse("<null>")
      s"ProxyResultFailure(" +
        s"cause=${cause.getClass.getName}, " +
        s"messageLength=${msg.length}, " +
        s"messagePrefix='${truncate(msg, OversizedPreviewChars)}'" +
        (if (stack.nonEmpty) s", stackHead=\n$stack" else "") +
        ")"
  }

  private def formatOversizedMessage(
      addressValue: String,
      bytes: Int,
      limit: Int,
      result: Option[ProxyResult],
      taskDescription: Option[HashedTaskDescription]
  ): String = {
    val taskLine = taskDescription match {
      case Some(d) =>
        s"\n  originating task: id=${d.taskId.id} version=${d.taskId.version} dataHash=${d.dataHash}"
      case None =>
        "\n  originating task: (not found in current state)"
    }
    val resultLine = result match {
      case Some(r) => s"\n  payload: ${describeResult(r)}"
      case None    => "\n  payload: (not present in state; stale snapshot?)"
    }
    s"ProxyResult for proxy '$addressValue' serializes to $bytes bytes which exceeds " +
      s"the DynamoDB per-transaction limit of $limit bytes. " +
      s"Return large task outputs via SharedFile instead of inlining them in the result." +
      taskLine +
      resultLine
  }

  private def attributeBytes(av: AttributeValue): Int = {
    var size = 0
    if (av.s() != null) size += av.s().getBytes("UTF-8").length
    if (av.n() != null) size += av.n().getBytes("UTF-8").length
    if (av.b() != null) size += av.b().asByteArray().length
    if (av.bool() != null) size += 1
    if (av.hasL)
      av.l().asScala.foreach(x => size += attributeBytes(x))
    if (av.hasM)
      av.m().asScala.foreach { case (k, v) =>
        size += k.getBytes("UTF-8").length
        size += attributeBytes(v)
      }
    if (av.hasSs)
      av.ss().asScala.foreach(x => size += x.getBytes("UTF-8").length)
    size
  }

  private def itemBytes(item: java.util.Map[String, AttributeValue]): Int =
    item.asScala.foldLeft(0) { case (acc, (k, v)) =>
      acc + k.getBytes("UTF-8").length + attributeBytes(v)
    }

  private[tasks] def estimateOpBytes(op: TransactWriteItem): Int = {
    if (op.put() != null) itemBytes(op.put().item())
    else if (op.delete() != null) itemBytes(op.delete().key()) + 64
    else if (op.conditionCheck() != null)
      itemBytes(op.conditionCheck().key()) + 64
    else if (op.update() != null) 256
    else 0
  }

}
