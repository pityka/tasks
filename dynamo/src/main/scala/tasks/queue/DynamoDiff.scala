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
  import DynamoQueueState.ResourcesPartitionKey
  import DynamoQueueState.ResourcesSortKey
  import DynamoQueueState.MaxOpsPerTransaction

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
      completedResults: Map[Address, ProxyResult],
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
      completedResults: Map[Address, ProxyResult],
      mainProcesses: Set[String],
      ver: Long
  ): Map[String, AttributeValue] = {
    val knownLaunchersMap: Map[String, AttributeValue] =
      knownLaunchers.map { case (name, nodeOpt) =>
        name.name -> b(encode(nodeOpt))
      }
    val countersMap: Map[String, AttributeValue] =
      counters.map { case (name, value) => name.name -> n(value) }
    val completedMap: Map[String, AttributeValue] =
      completedResults.map { case (address, result) =>
        address.value -> b(encode(result))
      }
    val mainProcList: List[AttributeValue] =
      mainProcesses.toList.sorted.map(s)
    Map(
      Attr.Pk -> s(ResourcesPartitionKey),
      Attr.Sk -> s(ResourcesSortKey),
      Attr.KnownLaunchers -> m(knownLaunchersMap),
      Attr.Counters -> m(countersMap),
      Attr.Nodes -> b(encode(nodes)),
      Attr.CompletedResults -> m(completedMap),
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
    val completedResults: Map[Address, ProxyResult] =
      attrs(Attr.CompletedResults)
        .m()
        .asScala
        .toMap
        .map { case (addr, av) =>
          Address(addr) -> decode[ProxyResult](av.b().asByteArray())
        }
    val mainProcesses: Set[String] =
      attrs(Attr.MainProcesses).l().asScala.toList.map(_.s()).toSet
    val ver = attrs(Attr.Ver).n().toLong
    (
      ResourcesFields(
        knownLaunchers = knownLaunchers,
        counters = counters,
        nodes = nodes,
        completedResults = completedResults,
        mainProcesses = mainProcesses
      ),
      ver
    )
  }

  def assembleState(
      taskMap: Map[String, (ScheduleTask, TaskItemFields, Long)],
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
      completedResults = resources.completedResults,
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
      completedResults = state.completedResults,
      mainProcesses = state.mainProcesses
    )

  private def resourcesEqual(a: ResourcesFields, b: ResourcesFields): Boolean =
    a == b

  private def putTaskItem(
      table: String,
      sk: String,
      item: TaskItemFields,
      ver: Long,
      condition: Option[(String, Map[String, AttributeValue])]
  ): TransactWriteItem = {
    val builder = Put
      .builder()
      .tableName(table)
      .item(encodeTaskAttrs(sk, item, ver).asJava)
    val configured = condition match {
      case None => builder
      case Some((expr, values)) =>
        builder
          .conditionExpression(expr)
          .expressionAttributeNames(
            Map("#ver" -> Attr.Ver, "#pk" -> Attr.Pk).asJava
          )
          .expressionAttributeValues(values.asJava)
    }
    TransactWriteItem.builder().put(configured.build()).build()
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
      completedResults = resources.completedResults,
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
      baseResourcesVersion: Long
  ): Plan = {
    val oldItems = taskItemsOf(oldState)
    val newItems = taskItemsOf(newState)
    val allKeys = oldItems.keySet ++ newItems.keySet

    val taskOps: List[TransactWriteItem] = allKeys.toList.sorted.flatMap { sk =>
      (oldItems.get(sk), newItems.get(sk)) match {
        case (Some(_), None) =>
          val baseVer = baseTaskVersions.getOrElse(sk, 0L)
          Some(deleteTaskItem(table, sk, baseVer))
        case (None, Some(item)) =>
          val condition = (
            "attribute_not_exists(#pk)",
            Map.empty[String, AttributeValue]
          )
          Some(putTaskItem(table, sk, item, 1L, Some(condition)))
        case (Some(oldItem), Some(newItem)) if oldItem == newItem => None
        case (Some(_), Some(newItem)) =>
          val baseVer = baseTaskVersions.getOrElse(sk, 0L)
          val condition = (
            "#ver = :ver",
            Map(":ver" -> n(baseVer))
          )
          Some(putTaskItem(table, sk, newItem, baseVer + 1, Some(condition)))
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

    if (taskOps.isEmpty && resourcesOp.isEmpty) Plan(Nil)
    else {
      val taskChunks: List[List[TransactWriteItem]] =
        if (taskOps.isEmpty) Nil
        else {
          val chunkSize =
            if (resourcesOp.isDefined) MaxOpsPerTransaction - 2
            else MaxOpsPerTransaction - 1
          taskOps.grouped(math.max(1, chunkSize)).toList
        }

      val resourcesBarrier =
        if (taskChunks.nonEmpty && resourcesOp.isEmpty)
          resourcesConditionCheck(table, baseResourcesVersion)
        else
          resourcesConditionCheck(table, baseResourcesVersion)

      val middleChunks: List[List[TransactWriteItem]] = (resourcesOp, taskChunks) match {
        case (_, Nil) => Nil
        case (None, chunks) =>
          chunks.map(chunk => chunk :+ resourcesBarrier)
        case (Some(resPut), chunks) =>
          val init = chunks.init
          val last = chunks.last
          init.map(chunk => chunk :+ resourcesBarrier) :+ (last :+ resPut)
      }

      val finalChunks: List[List[TransactWriteItem]] =
        if (middleChunks.isEmpty && resourcesOp.isDefined)
          List(List(resourcesOp.get))
        else middleChunks

      Plan(finalChunks)
    }
  }

}
