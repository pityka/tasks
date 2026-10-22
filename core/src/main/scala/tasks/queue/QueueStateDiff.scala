package tasks.queue

import tasks.elastic.NodeRegistryState
import tasks.queue.QueueImpl._
import tasks.queue.QueueStateCodecs._
import tasks.util.message.Address
import tasks.util.message.LauncherName
import tasks.util.message.MessageData.ScheduleTask
import tasks.util.message.Node
import tasks.util.transactional.TransactionalStateDiff

private[tasks] object QueueStateDiff {

  val StatusQueued: String = "Q"
  val StatusScheduled: String = "S"

  private val ChunkIndexWidth: Int = 6
  private[queue] val ChunkSeparator: String = "#"

  case class TaskItemFields(
      sch: ScheduleTask,
      status: String,
      proxies: List[Proxy],
      dispatches: List[Dispatch],
      placementValue: Option[String],
      resultDelivered: Boolean,
      enqueueTimestamp: Option[Long]
  )

  case class ResourcesFields(
      knownLaunchers: Map[LauncherName, Option[Node]],
      counters: Map[LauncherName, Long],
      nodes: NodeRegistryState.State,
      mainProcesses: Set[String],
      completedCount: Map[TaskId, Long],
      failedCount: Map[TaskId, Long],
      cacheHitCount: Map[TaskId, Long]
  )

  final case class Diff(
      taskOps: List[TransactionalStateDiff.Op[TaskItemFields]],
      resultChunkOps: List[TransactionalStateDiff.Op[Array[Byte]]],
      resourcesChange: Option[TransactionalStateDiff.Op[ResourcesFields]]
  ) {
    def isEmpty: Boolean =
      taskOps.isEmpty && resultChunkOps.isEmpty && resourcesChange.isEmpty
  }

  private val chunkIndexFormat: String = "%0" + ChunkIndexWidth + "d"

  private[queue] def chunkSk(addressValue: String, index: Int): String =
    addressValue + ChunkSeparator + chunkIndexFormat.format(index)

  private[queue] def parseAddressFromSk(sk: String): String =
    sk.substring(0, sk.lastIndexOf(ChunkSeparator))

  private def splitIntoChunks(
      bytes: Array[Byte],
      chunkPayloadBytes: Int
  ): List[Array[Byte]] =
    if (bytes.length <= chunkPayloadBytes) List(bytes)
    else bytes.grouped(chunkPayloadBytes).toList

  private def gzipBytes(bytes: Array[Byte]): Array[Byte] = {
    val out = new java.io.ByteArrayOutputStream()
    val gz = new java.util.zip.GZIPOutputStream(out)
    try gz.write(bytes)
    finally gz.close()
    out.toByteArray
  }

  private def gunzipBytes(bytes: Array[Byte]): Array[Byte] = {
    val gz = new java.util.zip.GZIPInputStream(
      new java.io.ByteArrayInputStream(bytes)
    )
    try gz.readAllBytes()
    finally gz.close()
  }

  private[queue] def completedResultChunks(
      completedResults: Map[Address, ProxyResult],
      chunkPayloadBytes: Int
  ): Map[String, Array[Byte]] =
    completedResults.flatMap { case (address, result) =>
      val serialized = gzipBytes(encode(result))
      splitIntoChunks(serialized, chunkPayloadBytes).zipWithIndex.map {
        case (chunk, i) => chunkSk(address.value, i) -> chunk
      }
    }

  private[queue] def assembleCompletedResults(
      chunkMap: Map[String, (Array[Byte], Long)]
  ): Map[Address, ProxyResult] = {
    val byAddress =
      chunkMap.toList.groupBy { case (sk, _) => parseAddressFromSk(sk) }
    byAddress.map { case (addressValue, entries) =>
      val sorted = entries.sortBy(_._1)
      val fullBytes =
        sorted.flatMap { case (_, (bytes, _)) => bytes }.toArray
      Address(addressValue) -> decode[ProxyResult](gunzipBytes(fullBytes))
    }
  }

  private[queue] def taskItemsOf(state: State): Map[String, TaskItemFields] = {
    val queued = state.queuedTasks.map { case (projection, (sch, proxies)) =>
      projection.description.hash -> TaskItemFields(
        sch = sch,
        status = StatusQueued,
        proxies = proxies,
        dispatches = Nil,
        placementValue = None,
        resultDelivered = false,
        enqueueTimestamp = state.enqueueTimestamps.get(projection)
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
          resultDelivered = scheduledTask.resultDelivered,
          enqueueTimestamp = None
        )
    }
    queued ++ scheduled
  }

  private[queue] def resourcesOf(state: State): ResourcesFields =
    ResourcesFields(
      knownLaunchers = state.knownLaunchers,
      counters = state.counters,
      nodes = state.nodes,
      mainProcesses = state.mainProcesses,
      completedCount = state.completedCount,
      failedCount = state.failedCount,
      cacheHitCount = state.cacheHitCount
    )

  private[queue] def assembleState(
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
    val enqueueTimestamps = taskMap.values
      .filter(_._2.status == StatusQueued)
      .flatMap { case (sch, fields, _) =>
        fields.enqueueTimestamp.map(ts => QueueImpl.project(sch) -> ts)
      }
      .toMap
    State(
      queuedTasks = queuedTasks,
      scheduledTasks = scheduledTasks,
      knownLaunchers = resources.knownLaunchers,
      counters = resources.counters,
      nodes = resources.nodes,
      completedResults = completedResults,
      mainProcesses = resources.mainProcesses,
      enqueueTimestamps = enqueueTimestamps,
      completedCount = resources.completedCount,
      failedCount = resources.failedCount,
      cacheHitCount = resources.cacheHitCount
    )
  }

  private[queue] def diff(
      oldState: State,
      newState: State,
      baseTaskVersions: Map[String, Long],
      baseResultChunkVersions: Map[String, Long],
      baseResourcesVersion: Long,
      resourcesSingletonKey: String,
      chunkPayloadBytes: Int
  ): Diff = {
    val taskOps = TransactionalStateDiff.diffCollection(
      oldItems = taskItemsOf(oldState),
      newItems = taskItemsOf(newState),
      baseVersions = baseTaskVersions
    )

    val oldChunks =
      completedResultChunks(oldState.completedResults, chunkPayloadBytes)
    val newChunks =
      completedResultChunks(newState.completedResults, chunkPayloadBytes)
    val resultChunkOps = TransactionalStateDiff.diffCollection(
      oldItems = oldChunks,
      newItems = newChunks,
      baseVersions = baseResultChunkVersions,
      equal = (a: Array[Byte], b: Array[Byte]) => java.util.Arrays.equals(a, b)
    )

    val resourcesChange = TransactionalStateDiff.diffSingleton(
      key = resourcesSingletonKey,
      oldItem = resourcesOf(oldState),
      newItem = resourcesOf(newState),
      baseVersion = baseResourcesVersion
    )

    Diff(taskOps, resultChunkOps, resourcesChange)
  }

  private[queue] def taskDescriptionForProxy(
      state: State,
      address: Address
  ): Option[HashedTaskDescription] = {
    val fromScheduled = state.scheduledTasks.values
      .find(_.proxies.exists(_.address == address))
      .map(_.sch.description)
    val fromQueued = state.queuedTasks.values.collectFirst {
      case (sch, proxies) if proxies.exists(_.address == address) =>
        sch.description
    }
    fromScheduled.orElse(fromQueued)
  }

}
