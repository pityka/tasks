package tasks.queue

import scala.jdk.CollectionConverters._

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model._

import tasks.elastic.NodeRegistryState
import tasks.queue.QueueImpl._
import tasks.queue.QueueStateCodecs._
import tasks.util.message.Address
import tasks.util.message.LauncherName
import tasks.util.message.MessageData.ScheduleTask
import tasks.util.message.Node
import tasks.util.transactional.TransactionalStateDiff

private[tasks] object DynamoDiff {

  import DynamoQueueState.Attr
  import DynamoQueueState.TasksPartitionKey
  import DynamoQueueState.ResultsPartitionKey
  import DynamoQueueState.ResourcesPartitionKey
  import DynamoQueueState.ResourcesSortKey
  import DynamoQueueState.MaxDataBytesPerTransaction
  import DynamoQueueState.MaxDataOpsPerChunk
  import DynamoQueueState.ChunkPayloadBytes

  type TaskItemFields = QueueStateDiff.TaskItemFields
  val TaskItemFields = QueueStateDiff.TaskItemFields

  type ResourcesFields = QueueStateDiff.ResourcesFields
  val ResourcesFields = QueueStateDiff.ResourcesFields

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

  private def taskIdKey(id: TaskId): String = s"${id.id}@${id.version}"

  private def parseTaskIdKey(k: String): TaskId = {
    val idx = k.lastIndexOf('@')
    TaskId(k.substring(0, idx), k.substring(idx + 1).toInt)
  }

  private[tasks] def gzip(bytes: Array[Byte]): Array[Byte] = {
    val out = new java.io.ByteArrayOutputStream()
    val gz = new java.util.zip.GZIPOutputStream(out)
    try gz.write(bytes)
    finally gz.close()
    out.toByteArray
  }

  private[tasks] def gunzip(bytes: Array[Byte]): Array[Byte] = {
    val gz = new java.util.zip.GZIPInputStream(
      new java.io.ByteArrayInputStream(bytes)
    )
    try gz.readAllBytes()
    finally gz.close()
  }

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
      Attr.Sch -> b(gzip(encode(item.sch))),
      Attr.Proxies -> l(proxiesList),
      Attr.Dispatches -> l(dispatchesList),
      Attr.ResultDelivered -> bool(item.resultDelivered),
      Attr.Ver -> n(ver)
    )
    val withPlacement = item.placementValue match {
      case None     => base
      case Some(pv) => base + (Attr.PlacementValue -> s(pv))
    }
    item.enqueueTimestamp match {
      case None     => withPlacement
      case Some(ts) => withPlacement + (Attr.EnqueueTimestamps -> n(ts))
    }
  }

  def decodeTaskItem(
      item: java.util.Map[String, AttributeValue]
  ): (String, (ScheduleTask, TaskItemFields, Long)) = {
    val attrs = item.asScala
    val sk = attrs(Attr.Sk).s()
    val status = attrs(Attr.Status).s()
    val sch = decode[ScheduleTask](gunzip(attrs(Attr.Sch).b().asByteArray()))
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
    val enqueueTimestamp =
      attrs.get(Attr.EnqueueTimestamps).map(_.n().toLong)
    val ver = attrs(Attr.Ver).n().toLong
    val fields = TaskItemFields(
      sch = sch,
      status = status,
      proxies = proxies,
      dispatches = dispatches,
      placementValue = placementValue,
      resultDelivered = resultDelivered,
      enqueueTimestamp = enqueueTimestamp
    )
    (sk, (sch, fields, ver))
  }

  def encodeResources(
      knownLaunchers: Map[LauncherName, Option[Node]],
      counters: Map[LauncherName, Long],
      nodes: NodeRegistryState.State,
      mainProcesses: Set[String],
      completedCount: Map[TaskId, Long],
      failedCount: Map[TaskId, Long],
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
    def taskIdLongMap(src: Map[TaskId, Long]): Map[String, AttributeValue] =
      src.map { case (taskId, c) => taskIdKey(taskId) -> n(c) }
    Map(
      Attr.Pk -> s(ResourcesPartitionKey),
      Attr.Sk -> s(ResourcesSortKey),
      Attr.KnownLaunchers -> m(knownLaunchersMap),
      Attr.Counters -> m(countersMap),
      Attr.Nodes -> b(gzip(encode(nodes))),
      Attr.MainProcesses -> l(mainProcList),
      Attr.CompletedCount -> m(taskIdLongMap(completedCount)),
      Attr.FailedCount -> m(taskIdLongMap(failedCount)),
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
      decode[NodeRegistryState.State](
        gunzip(attrs(Attr.Nodes).b().asByteArray())
      )
    val mainProcesses: Set[String] =
      attrs(Attr.MainProcesses).l().asScala.toList.map(_.s()).toSet

    def taskIdLongMap(key: String): Map[TaskId, Long] =
      attrs
        .get(key)
        .map(av =>
          av.m()
            .asScala
            .toMap
            .map { case (k, av) => parseTaskIdKey(k) -> av.n().toLong }
        )
        .getOrElse(Map.empty)

    val completedCount = taskIdLongMap(Attr.CompletedCount)
    val failedCount = taskIdLongMap(Attr.FailedCount)
    val ver = attrs(Attr.Ver).n().toLong
    (
      ResourcesFields(
        knownLaunchers = knownLaunchers,
        counters = counters,
        nodes = nodes,
        mainProcesses = mainProcesses,
        completedCount = completedCount,
        failedCount = failedCount
      ),
      ver
    )
  }

  private[tasks] def chunkSk(addressValue: String, index: Int): String =
    QueueStateDiff.chunkSk(addressValue, index)

  private[tasks] def parseAddressFromSk(sk: String): String =
    QueueStateDiff.parseAddressFromSk(sk)

  private def encodeResultChunkAttrs(
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
  ): Map[Address, ProxyResult] =
    QueueStateDiff.assembleCompletedResults(chunkMap)

  def assembleState(
      taskMap: Map[String, (ScheduleTask, TaskItemFields, Long)],
      completedResults: Map[Address, ProxyResult],
      resources: ResourcesFields
  ): State = QueueStateDiff.assembleState(taskMap, completedResults, resources)

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
      completedCount = resources.completedCount,
      failedCount = resources.failedCount,
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

  private def toTaskTransact(
      table: String,
      op: TransactionalStateDiff.Op[TaskItemFields]
  ): TransactWriteItem = op match {
    case TransactionalStateDiff.Insert(sk, item) =>
      putNewTaskItem(table, sk, item)
    case TransactionalStateDiff.Update(sk, item, baseVer) =>
      updateTaskItem(table, sk, item, baseVer)
    case TransactionalStateDiff.Delete(sk, baseVer) =>
      deleteTaskItem(table, sk, baseVer)
    case TransactionalStateDiff.CheckVersion(sk, baseVer) =>
      throw new IllegalStateException(
        s"CheckVersion is not generated for task items (sk=$sk, ver=$baseVer)"
      )
  }

  private def toResultChunkTransact(
      table: String,
      op: TransactionalStateDiff.Op[Array[Byte]]
  ): TransactWriteItem = op match {
    case TransactionalStateDiff.Insert(sk, chunk) =>
      putNewResultChunk(table, sk, chunk)
    case TransactionalStateDiff.Update(sk, chunk, baseVer) =>
      updateResultChunk(table, sk, chunk, baseVer)
    case TransactionalStateDiff.Delete(sk, baseVer) =>
      deleteResultChunk(table, sk, baseVer)
    case TransactionalStateDiff.CheckVersion(sk, baseVer) =>
      throw new IllegalStateException(
        s"CheckVersion is not generated for result chunks (sk=$sk, ver=$baseVer)"
      )
  }

  private def resourcesTerminalOp(
      table: String,
      op: TransactionalStateDiff.Op[ResourcesFields]
  ): TransactWriteItem = op match {
    case TransactionalStateDiff.Update(_, payload, baseVer) =>
      putResources(table, payload, baseVer + 1, baseVer)
    case other =>
      throw new IllegalStateException(
        s"diffSingleton on resources can only produce Update, got: $other"
      )
  }

  def plan(
      table: String,
      oldState: State,
      newState: State,
      baseTaskVersions: Map[String, Long],
      baseResultChunkVersions: Map[String, Long],
      baseResourcesVersion: Long
  ): Plan = {
    val diff = QueueStateDiff.diff(
      oldState = oldState,
      newState = newState,
      baseTaskVersions = baseTaskVersions,
      baseResultChunkVersions = baseResultChunkVersions,
      baseResourcesVersion = baseResourcesVersion,
      resourcesSingletonKey = ResourcesSortKey,
      chunkPayloadBytes = ChunkPayloadBytes
    )

    val taskUnits: List[TransactionalStateDiff.WriteUnit[TransactWriteItem]] =
      diff.taskOps.map { op =>
        val item = toTaskTransact(table, op)
        val bytes = estimateOpBytes(item)
        if (bytes > DynamoQueueState.MaxItemBytes) {
          throw new DynamoQueueState.OversizedItemException(
            formatOversizedTaskItemMessage(
              sk = op.key,
              bytes = bytes,
              limit = DynamoQueueState.MaxItemBytes,
              task = taskFieldsFromOp(oldState, newState, op)
            )
          )
        }
        TransactionalStateDiff.WriteUnit(List(item), bytes)
      }

    val resultUnits
        : List[TransactionalStateDiff.WriteUnit[TransactWriteItem]] = {
      val grouped: Map[String, List[TransactionalStateDiff.Op[Array[Byte]]]] =
        diff.resultChunkOps.groupBy(op =>
          QueueStateDiff.parseAddressFromSk(op.key)
        )
      grouped.toList.map { case (addressValue, opsForAddress) =>
        val items = opsForAddress.map(op => toResultChunkTransact(table, op))
        val bytes = items.map(estimateOpBytes).sum
        if (bytes > MaxDataBytesPerTransaction) {
          val address = Address(addressValue)
          val resultOpt = newState.completedResults
            .get(address)
            .orElse(oldState.completedResults.get(address))
          val taskHint =
            QueueStateDiff
              .taskDescriptionForProxy(oldState, address)
              .orElse(QueueStateDiff.taskDescriptionForProxy(newState, address))
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
        TransactionalStateDiff.WriteUnit(items, bytes)
      }
    }

    val units = taskUnits ++ resultUnits

    val dataChunks: List[List[TransactWriteItem]] =
      if (units.isEmpty) Nil
      else
        TransactionalStateDiff.packUnits(
          units = units,
          maxOpsPerChunk = MaxDataOpsPerChunk,
          maxBytesPerChunk = MaxDataBytesPerTransaction,
          describeOversized = (u: TransactionalStateDiff.WriteUnit[
            TransactWriteItem
          ]) =>
            s"DynamoDB TransactWriteItems unit of ${u.bytes} bytes " +
              s"exceeds the per-transaction limit of $MaxDataBytesPerTransaction bytes " +
              s"(${u.ops.size} ops)"
        )

    val barrier = resourcesConditionCheck(table, baseResourcesVersion)
    val terminal = diff.resourcesChange.map(resourcesTerminalOp(table, _))
    terminal.foreach { op =>
      val bytes = estimateOpBytes(op)
      if (bytes > DynamoQueueState.MaxItemBytes)
        throw new DynamoQueueState.OversizedItemException(
          s"The DynamoDB resources singleton item serializes to $bytes bytes " +
            s"which exceeds the DynamoDB per-item limit of ${DynamoQueueState.MaxItemBytes} bytes. " +
            s"knownLaunchers=${newState.knownLaunchers.size}, " +
            s"counters=${newState.counters.size}, " +
            s"mainProcesses=${newState.mainProcesses.size}, " +
            s"completedCount=${newState.completedCount.size}, " +
            s"failedCount=${newState.failedCount.size}, " +
            s"nodes.running=${newState.nodes.running.size}, " +
            s"nodes.pending=${newState.nodes.pending.size}, " +
            s"nodes.inFlightRequests=${newState.nodes.inFlightRequests.size}."
        )
    }

    Plan(
      TransactionalStateDiff.withSingletonBarrier(dataChunks, barrier, terminal)
    )
  }

  private def taskFieldsFromOp(
      oldState: State,
      newState: State,
      op: TransactionalStateDiff.Op[TaskItemFields]
  ): Option[(ScheduleTask, String)] = {
    val sk = op.key
    val fromNew =
      QueueStateDiff.taskItemsOf(newState).get(sk).map(f => (f.sch, f.status))
    val fromOld =
      QueueStateDiff.taskItemsOf(oldState).get(sk).map(f => (f.sch, f.status))
    fromNew.orElse(fromOld)
  }

  private def formatOversizedTaskItemMessage(
      sk: String,
      bytes: Int,
      limit: Int,
      task: Option[(ScheduleTask, String)]
  ): String = {
    val taskLine = task match {
      case Some((sch, status)) =>
        val d = sch.description
        val rawSchBytes = encode(sch).length
        val compressedSchBytes = gzip(encode(sch)).length
        s"\n  task: id=${d.taskId.id} version=${d.taskId.version} " +
          s"dataHash=${d.dataHash} status=$status " +
          s"serializedScheduleTaskBytes=$rawSchBytes " +
          s"compressedScheduleTaskBytes=$compressedSchBytes " +
          s"inputBase64Length=${sch.input.b64.value.length} " +
          s"inputBase64Prefix='${truncate(sch.input.b64.value, OversizedPreviewChars)}'"
      case None =>
        s"\n  task sk=$sk (full task not found in state snapshot)"
    }
    s"DynamoDB task item for sk=$sk serializes to $bytes bytes (after gzip) " +
      s"which exceeds the DynamoDB per-item limit of $limit bytes. " +
      s"The ScheduleTask (including its serialized input) is too large to persist as one DDB item even compressed. " +
      s"Move the heavy parts of the task input (bulk bytes) behind a SharedFile reference instead of inlining them." +
      taskLine
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
        .map(e =>
          s"    at ${e.getClassName}.${e.getMethodName}(${e.getFileName}:${e.getLineNumber})"
        )
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

  private def estimateOpBytes(op: TransactWriteItem): Int = {
    if (op.put() != null) itemBytes(op.put().item())
    else if (op.delete() != null) itemBytes(op.delete().key()) + 64
    else if (op.conditionCheck() != null)
      itemBytes(op.conditionCheck().key()) + 64
    else if (op.update() != null) 256
    else 0
  }

}
