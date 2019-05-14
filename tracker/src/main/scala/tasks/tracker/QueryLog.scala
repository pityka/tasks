package tasks.tracker

import tasks.shared._
import java.io.InputStream

object QueryLog {

  val cpuTimeKey = "__cpuTime"
  val wallClockTimeKey = "__wallClockTime"
  val cpuNeedKey = "__cpuNeed"
  val multiplicityKey = "__multiplicity"

  object Node {
    import io.circe._
    import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
    implicit val encoder: Encoder[Node] =
      deriveEncoder[Node]
    implicit val decoder: Decoder[Node] =
      deriveDecoder[Node]
  }

  case class RawNode(taskId: String,
                     labels: Labels,
                     pathFromRoot: Seq[String],
                     resource: ResourceAllocated,
                     elapsedTime: ElapsedTimeNanoSeconds,
                     dataDependencies: Seq[String]) {
    def id = pathFromRoot.last
  }

  object RawNode {
    import io.circe._
    import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
    implicit val encoder: Encoder[RawNode] =
      deriveEncoder[RawNode]
    implicit val decoder: Decoder[RawNode] =
      deriveDecoder[RawNode]
  }

  case class Node(taskId: String,
                  id: String,
                  labels: Labels,
                  dataChildren: Seq[String],
                  directChildren: Seq[String],
                  resource: ResourceAllocated,
                  elapsedTime: ElapsedTimeNanoSeconds) {

    def bothChildren = (dataChildren ++ directChildren).distinct

    def cpuNeed =
      labels.values.find(_._1 == cpuNeedKey).map(_._2.toDouble.toInt)
    def cpuTime = labels.values.find(_._1 == cpuTimeKey).map(_._2.toDouble)
    def wallClockTime =
      labels.values.find(_._1 == wallClockTimeKey).map(_._2.toDouble)
  }

  def readNodes(source: InputStream,
                excludeTaskIds: Set[String],
                includeTaskIds: Set[String]): Seq[RawNode] =
    scala.io.Source
      .fromInputStream(source)
      .getLines
      .map { line =>
        val parsed = io.circe.parser.decode[ResourceUtilizationRecord](line)
        // if (parsed.isLeft) {
        // println(parsed.left.get + " " + line)
        // }
        parsed.right
      }
      .filter(_.toOption.isDefined)
      .map(_.get)
      .filterNot(elem =>
        if (excludeTaskIds.isEmpty) false
        else excludeTaskIds.contains(elem.taskId.id))
      .filter(elem =>
        if (includeTaskIds.isEmpty) true
        else includeTaskIds.contains(elem.taskId.id))
      .filter(elem => elem.metadata.isDefined)
      .map(elem =>
        RawNode(
          elem.taskId.id,
          elem.labels,
          "root" +: elem.metadata.get.lineage.lineage.map(_.toString),
          elem.resource,
          elem.elapsedTime,
          elem.metadata.toSeq
            .flatMap(_.dependencies.flatMap(_.context.toSeq.collect {
              case h: tasks.fileservice.HistoryContextImpl => h.traceId.toList
            }.flatten))
            .distinct
      ))
      .toList

  def subtree(tree: Seq[RawNode], root: String) =
    tree.filter(_.pathFromRoot.contains(root))

  private def toEdgeList(tree: Seq[RawNode]): Seq[Node] = {
    val forwardEdges = tree
      .flatMap { node =>
        if (node.pathFromRoot.size > 2)
          node.pathFromRoot.drop(1).sliding(2).toList.map { group =>
            (group(0) -> group(1))
          } else Nil
      }
      .groupBy(_._1)
    tree.map { raw =>
      val id = raw.id
      val children = forwardEdges.get(id).getOrElse(Nil).map(_._2)
      Node(
        taskId = raw.taskId,
        id = id,
        labels = raw.labels,
        directChildren = children.distinct,
        dataChildren = Nil,
        resource = raw.resource,
        elapsedTime = raw.elapsedTime
      )
    }
  }

  private def topologicalSort(nodes: Seq[Node]): Seq[Node] = {
    val byId = nodes.groupBy(_.id)
    val forwardEdges = byId.map {
      case (id, group) => (id, group.head.bothChildren)
    }
    var order = List.empty[Node]
    var marks = Set.empty[String]
    var currentParents = Set.empty[String]

    def visit(n: Node): Unit =
      if (marks.contains(n.id)) ()
      else {
        if (currentParents.contains(n.id)) {
          println(s"error: loop to ${n.id}")
          ()
        } else {
          currentParents = currentParents + n.id
          val children = forwardEdges.get(n.id).toSeq.flatten.flatMap {
            childrenId =>
              byId(childrenId)
          }
          children.foreach(visit)
          currentParents = currentParents - n.id
          marks = marks + n.id
          order = n :: order
        }
      }

    nodes.foreach { node =>
      if (!marks.contains(node.id)) {
        visit(node)
      }
    }

    order

  }

  private def addDataEdgesIfNotCausingCycles(
      tree: Seq[Node],
      extraForwardEdges: Map[String, List[String]]) = {
    val children = recursiveChildren(tree)

    tree.map { node =>
      val candidateDataChildren = extraForwardEdges
        .get(node.id)
        .toSeq
        .flatten
        .distinct

      val edgesNotIntroducingCycles =
        candidateDataChildren.filterNot { ch =>
          val allDirectDependencies = children(ch).toSet

          allDirectDependencies.contains(node.id)
        }

      node.copy(dataChildren = edgesNotIntroducingCycles)
    }
  }

  def recursiveChildrenBothTypes(tree: Seq[Node]): Map[String, Seq[String]] = {
    val sorted = topologicalSort(tree).reverse
    sorted.foldLeft(Map.empty[String, Seq[String]]) {
      case (map, elem) =>
        val allChildren = (elem.bothChildren ++ elem.bothChildren.flatMap(ch =>
          map.get(ch).toSeq.flatten)).distinct
        map.updated(elem.id, allChildren)
    }
  }
  def recursiveChildren(tree: Seq[Node]): Map[String, Seq[String]] = {
    val sorted = topologicalSort(tree).reverse
    sorted.foldLeft(Map.empty[String, Seq[String]]) {
      case (map, elem) =>
        val allChildren =
          (elem.directChildren ++ elem.directChildren.flatMap(ch =>
            map.get(ch).toSeq.flatten)).distinct
        map.updated(elem.id, allChildren)
    }
  }

  private def aggregateRuntime(tree: Seq[Node]) = {
    val sorted = topologicalSort(tree).reverse
    val byId = tree.groupBy(_.id).map(x => x._1 -> x._2.head)
    val wallClockTime = scala.collection.mutable.Map[String, Double]()
    val cpuNeed = scala.collection.mutable.Map[String, Seq[Node]]()
    val cpuTime = scala.collection.mutable.Map[String, Double]()

    def max(l: Seq[Double]) = if (l.isEmpty) None else Some(l.max)

    val allChildren = recursiveChildrenBothTypes(tree)

    sorted.foreach { node =>
      val maxWallClockTimeOfChildren = max(
        node.bothChildren
          .map { ch =>
            wallClockTime(ch)
          }).getOrElse(0d)

      val wallClockTime0 = node.elapsedTime.toDouble + maxWallClockTimeOfChildren

      val cpuNeed0 = {
        val dependentsNeededToComplete =
          node.bothChildren.flatMap(ch => cpuNeed(ch)).distinct
        val cpuUsedByDependents =
          dependentsNeededToComplete.map(_.resource.cpu).sum
        if (cpuUsedByDependents > node.resource.cpu.toDouble)
          dependentsNeededToComplete
        else List(node)
      }

      val cpuTime0 = node.elapsedTime.toDouble * node.resource.cpu + allChildren(
        node.id).map(byId).map(n => n.elapsedTime.toDouble * n.resource.cpu).sum

      wallClockTime.update(node.id, wallClockTime0)
      cpuNeed.update(node.id, cpuNeed0)
      cpuTime.update(node.id, cpuTime0)

    }

    tree.map { node =>
      node.copy(
        labels = node.labels ++ Labels(List(
          cpuTimeKey -> (cpuTime(node.id)).toString,
          cpuNeedKey -> cpuNeed(node.id).map(_.resource.cpu).sum.toString,
          wallClockTimeKey -> (wallClockTime(node.id)).toString
        )))
    }

  }

  private def collapseMultiEdges(tree: Seq[Node]) = {
    val sorted = topologicalSort(tree)
    val byId = tree.groupBy(_.id).map(x => x._1 -> x._2.head)
    val blacklist = scala.collection.mutable.Set[String]()
    sorted.flatMap { parent =>
      if (blacklist.contains(parent.id)) Nil
      else {
        val dataChildrenByTaskId =
          parent.dataChildren.map(byId).groupBy(_.taskId)
        val data1 = dataChildrenByTaskId.flatMap {
          case (_, group) =>
            val head = group.sortBy(_.id).head
            group.sortBy(_.id).drop(1).foreach { n =>
              blacklist += n.id
            }
            List.fill(group.size)(head.id)
        }
        val directChildrenByTaskId =
          parent.directChildren.map(byId).groupBy(_.taskId)
        val direct1 = directChildrenByTaskId.flatMap {
          case (_, group) =>
            val head = group.sortBy(_.id).head
            group.sortBy(_.id).drop(1).foreach { n =>
              blacklist += n.id
            }
            List.fill(group.size)(head.id)
        }
        List(
          parent.copy(dataChildren = data1.toList,
                      directChildren = direct1.toList))
      }
    }

  }

  def ancestorsFinished(tree: Seq[RawNode]) = {
    val ids = tree.groupBy(_.id).map { case (id, nodes) => (id, nodes.head) }
    tree.filter(node =>
      node.pathFromRoot.forall(id =>
        ids.contains(id) || node.pathFromRoot.head == id))
  }

  def formatTime(nanos: Double, seconds: Boolean = false) = {
    val div = if (seconds) 1 else 3600
    val hours = nanos * 1E-9 / div
    f"$hours%.1f"
  }

  def dot(s: Seq[Node], seconds: Boolean) = {
    val nodelist = s
      .map {
        case node =>
          val labels = node.labels.values.toMap
          val cpuTime = labels(cpuTimeKey).toDouble
          val wallClockTime = labels(wallClockTimeKey).toDouble
          val cpuNeed = labels(cpuNeedKey).toDouble

          val timeUnit = if (seconds) "s" else "h"

          s""""${node.id}" [label="${node.taskId}(${formatTime(
            node.elapsedTime.toDouble,
            seconds)}$timeUnit,${formatTime(wallClockTime, seconds)}wc$timeUnit,${formatTime(
            cpuTime,
            seconds)}c$timeUnit,${node.resource.cpu}c,${cpuNeed}C)"] """
      }
      .mkString(";")
    val edgelist = s
      .flatMap {
        case parent =>
          val directEdges = parent.directChildren
            .map { directChild =>
              (parent.id, directChild)
            }
            .groupBy(identity)
            .toSeq
            .map {
              case ((parent, child), group) =>
                val multiplicityLabel =
                  if (group.size < 2) "" else "[label= \"" + group.size + "x\"]"
                s""""${parent}" -> "${child}" $multiplicityLabel """
            }
          val dataEdges = parent.dataChildren
            .map { directChild =>
              (parent.id, directChild)
            }
            .groupBy(identity)
            .toSeq
            .map {
              case ((parent, child), group) =>
                val multiplicityLabel =
                  if (group.size < 2) "[color=\"red\"]"
                  else "[color=\"red\" label= \"" + group.size + "x\"]"
                s""""${parent}" -> "${child}" $multiplicityLabel """
            }

          directEdges ++ dataEdges
      }
      .mkString(";")

    s"""digraph tasks {$nodelist;$edgelist}"""
  }

  def computeRuntimes(allNodes: Seq[RawNode], subtree: Option[String]) = {
    val selectedTree = {
      val onlyFinished = ancestorsFinished(allNodes)

      subtree match {
        case None    => onlyFinished
        case Some(s) => QueryLog.subtree(onlyFinished, s)
      }
    }

    val forwardEdgesFromDataDependencies = allNodes
      .flatMap { n =>
        n.dataDependencies.map { d =>
          (n.id, d)
        }
      }
      .distinct
      .groupBy(_._1)
      .map { case (id, group) => (id, group.map(_._2).toList) }

    val edgeList = toEdgeList(selectedTree)

    val augmented =
      addDataEdgesIfNotCausingCycles(edgeList, forwardEdgesFromDataDependencies)

    aggregateRuntime(
      augmented
    )

  }

  def plotTimes(timesComputed: Seq[Node], seconds: Boolean) = {
    val collapsed = collapseMultiEdges(timesComputed)
    dot(collapsed, seconds)
  }

}
