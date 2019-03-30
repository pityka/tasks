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
            _.dependencies.dependencies.flatMap(_.context.toSeq.collect {
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

  def aggregateRuntime(tree: Seq[Node],
                       dependentSiblings: Map[String, List[String]]) = {
    val forwardEdges = tree.groupBy(_.parent)
    val sorted = topologicalSort(tree, forwardEdges).reverse
    val wallClockTime = scala.collection.mutable.Map[String, Double]()
    val cpuNeed = scala.collection.mutable.Map[String, Double]()
    val cpuTime = scala.collection.mutable.Map[String, Double]()

    def max(l: Seq[Double]) = if (l.isEmpty) None else Some(l.max)

    sorted.foreach { node =>
      val children = forwardEdges.get(node.id).toSeq.flatten
      val childrenByTaskId = children.groupBy(_.taskId)

      // graph among siblings
      // edges in this graph are of `dependentSiblings`
      val childrenGraph = children
        .map { ch =>
          val outTaskIds = dependentSiblings.get(ch.taskId).toList.flatten
          val outNodes =
            outTaskIds.flatMap(t => childrenByTaskId.get(t).toSeq).flatten
          (ch.id, outNodes)
        }
        .filter(_._2.nonEmpty)
        .toMap
      val childrenTopologicalOrder =
        topologicalSort(children, childrenGraph).reverse

      childrenTopologicalOrder.foreach { node =>
        val children = childrenGraph.get(node.id).toSeq.flatten
        val wallClockTime1 = wallClockTime(node.id) + max(
          children.map(ch => wallClockTime(ch.id))).getOrElse(0d)
        wallClockTime.update(node.id, wallClockTime1)
      }

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

  def formatTime(nanos: Double) = {
    val hours = nanos * 1E-9 / 3600
    f"$hours%.1f"
  }

  def dot(s: Seq[Node], extraEdges: List[(String, String)]) = {
    val nodelist = s
      .map { node =>
        val labels = node.labels.values.toMap
        val cpuTime = labels(cpuTimeKey).toDouble
        val wallClockTime = labels(wallClockTimeKey).toDouble
        val cpuNeed = labels(cpuNeedKey).toDouble

        s""""${node.id}" [label="${node.taskId}(${formatTime(
          node.elapsedTime.toDouble)}h,${formatTime(wallClockTime)}wch,${formatTime(
          cpuTime)}ch,${node.resource.cpu}c,${cpuNeed}C)"] """
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

    val extraEdgeList = extraEdges
      .map {
        case (from, to) =>
          s""""${from}" -> "${to}" [color="red"] """
      }
      .mkString(";")

    s"""digraph tasks {$nodelist;$edgelist;$extraEdgeList}"""
  }

  def plotDependencyGraph(allNodes: Seq[Node],
                          subtree: Option[String],
                          extraDependencies: Map[String, List[String]]) = {
    val selectedTree = {
      val onlyFinished = ancestorsFinished(allNodes)

      subtree match {
        case None    => onlyFinished
        case Some(s) => QueryLog.subtree(onlyFinished, s)
      }
    }

    val timesComputed = aggregateRuntime(
      selectedTree,
      extraDependencies
    )

    val collapsed = collapseMultiEdges(timesComputed)
    dot(collapsed,
        extraDependencies.toSeq
          .flatMap(x => x._2.map(y => x._1 -> y))
          .toList)
  }

}
