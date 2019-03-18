package tasks.tracker

import tasks.shared._
import java.io.InputStream

object QueryLog {

  case class Node(taskId: String,
                  labels: Labels,
                  pathFromRoot: Seq[String],
                  resource: ResourceAllocated,
                  elapsedTime: ElapsedTimeNanoSeconds) {
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
          elem.elapsedTime
      ))
      .toList

  def trees(s: Seq[Node]): Map[String, Seq[Node]] = s.groupBy(_.root)

  def subtree(tree: Seq[Node], root: String) =
    tree.filter(_.pathFromRoot.contains(root))

  val multiplicityKey = "__multiplicity"

  def collapseMultiEdges(tree: Seq[Node])(
      aggregate: Seq[Node] => (ResourceAllocated, ElapsedTimeNanoSeconds)) = {
    val by = (n: Node) => (n.taskId -> n.parent)
    val groups = tree.groupBy(by)
    val ids = tree.groupBy(_.id).map { case (id, nodes) => (id, nodes.head) }
    tree.map { node =>
      val transformedPath = node.pathFromRoot.map { id =>
        ids.get(id).map(_.taskId).getOrElse("root")
      }
      val group = groups(by(node))
      val (aggregatedResource, aggregatedTime) = aggregate(group)

      node.copy(
        pathFromRoot = transformedPath,
        resource = aggregatedResource,
        elapsedTime = aggregatedTime,
        labels = node.labels ++ Labels(
          List(multiplicityKey -> group.size.toString))
      )
    }.distinct
  }

  def formatTime(node: Node) = {
    val hours = node.elapsedTime * 1E-9 / 3600
    f"$hours%.1f"
  }

  def dot(s: Seq[Node]) = {
    val nodelist = s
      .map { node =>
        s""""${node.id}" [label="${node.taskId}(${formatTime(node)}h,${node.resource.cpu}c)"] """
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

    s"""digraph tasks {$nodelist;$edgelist;}"""
  }

}
