package tasks.tracker

import tasks.shared._
import java.io.InputStream

object QueryLog {

  val cpuTimeKey = "__cpuTime"
  val wallClockTimeKey = "__wallClockTime"
  val cpuNeedKey = "__cpuNeed"
  val multiplicityKey = "__multiplicity"

  case class Node(taskId: String,
                  labels: Labels,
                  pathFromRoot: Seq[String],
                  resource: ResourceAllocated,
                  elapsedTime: ElapsedTimeNanoSeconds,
                  dependencies: Seq[String]) {
    def id = pathFromRoot.last

    /* The root is not present */
    def parent = pathFromRoot.dropRight(1).last
    def root = pathFromRoot.head

    def cpuNeed =
      labels.values.find(_._1 == cpuNeedKey).map(_._2.toDouble.toInt)
    def cpuTime = labels.values.find(_._1 == cpuTimeKey).map(_._2.toDouble)
    def wallClockTime =
      labels.values.find(_._1 == wallClockTimeKey).map(_._2.toDouble)
  }

  def readNodes(source: InputStream,
                excludeTaskIds: Set[String],
                includeTaskIds: Set[String]): Seq[Node] =
    scala.io.Source
      .fromInputStream(source)
      .getLines
      .map { line =>
        io.circe.parser.decode[ResourceUtilizationRecord](line).right.get
      }
      .filterNot(elem =>
        if (excludeTaskIds.isEmpty) false
        else excludeTaskIds.contains(elem.taskId.id))
      .filter(elem =>
        if (includeTaskIds.isEmpty) true
        else includeTaskIds.contains(elem.taskId.id))
      .filter(elem => elem.labels.values.find(_._1 == Labels.traceKey).nonEmpty)
      .map(elem =>
        Node(
          elem.taskId.id,
          Labels(elem.labels.values.filterNot(_._1 == Labels.traceKey)),
          elem.labels.values.toMap.apply(Labels.traceKey).split("::").toList,
          elem.resource,
          elem.elapsedTime,
          elem.metadata.toSeq.flatMap(
            _.dependencies.flatMap(_.context.toSeq.collect {
              case h: tasks.fileservice.HistoryContextImpl => h.task.taskID
            }))
      ))
      .toList

  def trees(s: Seq[Node]): Map[String, Seq[Node]] = s.groupBy(_.root)

  def subtree(tree: Seq[Node], root: String) =
    tree.filter(_.pathFromRoot.contains(root))

  def topologicalSort(tree: Seq[Node],
                      forwardEdges: Map[String, Seq[Node]]): Seq[Node] = {
    var order = List.empty[Node]
    var marks = Set.empty[String]

    def visit(n: Node): Unit =
      if (marks.contains(n.id)) ()
      else {
        val children = forwardEdges.get(n.id).toSeq.flatten
        children.foreach(visit)
        marks = marks + n.id
        order = n :: order
      }

    tree.foreach { node =>
      if (!marks.contains(node.id)) {
        visit(node)
      }
    }

    order

  }

  def filterDataDependenciesForSiblings(
      tree: Seq[Node],
      extraDependencies: Map[String, List[String]]) = {
    val forwardEdges = tree.groupBy(_.parent)
    val siblings = forwardEdges.flatMap {
      case (_, siblings) =>
        siblings.map(n => n.id -> siblings)
    }
    val byTaskId = tree.groupBy(_.taskId)
    extraDependencies
      .map {
        case (taskId, taskIdChildren) =>
          val siblingTaskIds = byTaskId(taskId).flatMap { n =>
            siblings.get(n.id).toSeq.flatten.map(_.taskId)
          }
          taskId -> taskIdChildren.filter(siblingTaskIds.toSet)
      }
      .filter(_._2.nonEmpty)
  }

  def addDependencyAmongSiblings(
      tree: Seq[Node],
      dependentSiblings: Map[String, List[String]]) = {
    val forwardEdges = tree.groupBy(_.parent)
    val sorted = topologicalSort(tree, forwardEdges).reverse
    sorted.map { node =>
      val parent = node.parent
      val siblings = forwardEdges(parent)
      val dependsOnSibling = siblings.find(s =>
        dependentSiblings.get(s.taskId).toSeq.flatten.contains(node.taskId))
      dependsOnSibling match {
        case None => node
        case Some(newParent) =>
          node.copy(
            pathFromRoot = node.pathFromRoot
              .dropRight(2) :+ newParent.id :+ node.id)
      }

    }
  }

  def aggregateRuntime(tree: Seq[Node]) = {
    val forwardEdges = tree.groupBy(_.parent)
    val sorted = topologicalSort(tree, forwardEdges).reverse
    val wallClockTime = scala.collection.mutable.Map[String, Double]()
    val cpuNeed = scala.collection.mutable.Map[String, Double]()
    val cpuTime = scala.collection.mutable.Map[String, Double]()

    def max(l: Seq[Double]) = if (l.isEmpty) None else Some(l.max)

    sorted.foreach { node =>
      val children = forwardEdges.get(node.id).toSeq.flatten

      val maxWallClockTimeOfChildren = max(
        children
          .map { ch =>
            wallClockTime(ch.id)
          }).getOrElse(0d)

      val wallClockTime0 = node.elapsedTime.toDouble + maxWallClockTimeOfChildren

      val cpuNeed0 = math.max(node.resource.cpu.toDouble,
                              children.map(ch => cpuNeed(ch.id)).sum)
      val cpuTime0 = node.elapsedTime * node.resource.cpu + children
        .map(ch => cpuTime(ch.id))
        .sum

      wallClockTime.update(node.id, wallClockTime0)
      cpuNeed.update(node.id, cpuNeed0)
      cpuTime.update(node.id, cpuTime0)

    }

    tree.map { node =>
      node.copy(
        labels = node.labels ++ Labels(
          List(
            cpuTimeKey -> (cpuTime(node.id)).toString,
            cpuNeedKey -> cpuNeed(node.id).toString,
            wallClockTimeKey -> (wallClockTime(node.id)).toString
          )))
    }

  }

  def collapseMultiEdges(tree: Seq[Node]) = {
    val by = (n: Node) => (n.taskId -> n.parent)
    val ids = tree.groupBy(_.id).map { case (id, nodes) => (id, nodes.head) }
    tree
      .groupBy(by)
      .toSeq
      .map {
        case ((_, _), group) =>
          val representative = group.head
          val transformedPath = representative.pathFromRoot.map { id =>
            ids.get(id).map(_.taskId).getOrElse("root")
          }

          representative.copy(
            pathFromRoot = transformedPath,
            labels = representative.labels ++ Labels(
              List(multiplicityKey -> group.size.toString))
          )

      }

  }

  def ancestorsFinished(tree: Seq[Node]) = {
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
      .map { node =>
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
      .map { node =>
        val multiplicityLabel =
          node.labels.values.toMap.get(multiplicityKey) match {
            case None      => ""
            case Some("1") => ""
            case Some(x)   => "[label= \"" + x + "x\"]"
          }
        s""""${node.parent}" -> "${node.id}" $multiplicityLabel """
      }
      .mkString(";")

    s"""digraph tasks {$nodelist;$edgelist}"""
  }

  def computeRuntimes(allNodes: Seq[Node], subtree: Option[String]) = {
    val selectedTree = {
      val onlyFinished = ancestorsFinished(allNodes)

      subtree match {
        case None    => onlyFinished
        case Some(s) => QueryLog.subtree(onlyFinished, s)
      }
    }

    val extraDependencies = selectedTree
      .flatMap { n =>
        n.dependencies.map { d =>
          (d, n.taskId)
        }
      }
      .distinct
      .groupBy(_._1)
      .map { case (id, group) => (id, group.map(_._2).toList) }

    val filteredExtraDependencies =
      filterDataDependenciesForSiblings(selectedTree, extraDependencies)

    val augmented =
      addDependencyAmongSiblings(selectedTree, filteredExtraDependencies)

    aggregateRuntime(
      augmented
    )

  }

  def plotTimes(timesComputed: Seq[Node], seconds: Boolean) = {
    val collapsed = collapseMultiEdges(timesComputed)
    dot(collapsed, seconds)
  }

}
