package tasks.elastic

import cats.effect.IO
import cats.effect.ExitCode
import cats.effect.kernel.Deferred

import tasks.deploy.HostConfiguration
import tasks.shared._
import tasks.util.SimpleSocketAddress
import tasks.util.config.TasksConfig
import tasks.util.message.Node

final case class CompositeMember(
    name: String,
    support: ElasticSupport,
    accepts: ResourceRequest => Boolean,
    owns: String => Boolean
) {

  require(name.nonEmpty, "CompositeMember.name must be non-empty")

  def accepting(predicate: ResourceRequest => Boolean): CompositeMember =
    copy(accepts = predicate)

  def owning(predicate: String => Boolean): CompositeMember =
    copy(owns = predicate)

  def isCatchAll: Boolean = owns eq CompositeMember.anyNodeId

  override def toString: String =
    s"CompositeMember($name${if (isCatchAll) ", catch all" else ""})"
}

object CompositeMember {

  val anyNodeId: String => Boolean = _ => true

  def apply(
      name: String,
      support: ElasticSupport,
      owns: String => Boolean
  ): CompositeMember =
    CompositeMember(name, support, _ => true, owns)

  def catchAll(name: String, support: ElasticSupport): CompositeMember =
    CompositeMember(name, support, _ => true, anyNodeId)
}

object CompositeElasticSupport {

  def apply(members: List[CompositeMember]): ElasticSupport =
    apply(members, members.exists(_.support.needsPackageServer))

  def apply(
      members: List[CompositeMember],
      needsPackageServer: Boolean
  ): ElasticSupport = {
    validate(members)
    new ElasticSupport(
      hostConfig = Some(hostConfigOf(members)),
      shutdownFromNodeRegistry = new CompositeShutdownNode(members),
      shutdownFromWorker = new CompositeShutdownSelfNode(members),
      createNodeFactory = new CompositeCreateNodeFactory(members),
      getNodeName = new CompositeGetNodeName(members),
      convertRunningToPending = new CompositeConvertRunningToPending(members),
      needsPackageServer = needsPackageServer
    )
  }

  private def validate(members: List[CompositeMember]): Unit = {
    require(
      members.nonEmpty,
      "CompositeElasticSupport needs at least one member"
    )
    require(
      members.map(_.name).distinct.size == members.size,
      s"CompositeElasticSupport member names must be unique, got ${members.map(_.name).mkString(", ")}"
    )
    require(
      members.dropRight(1).forall(!_.isCatchAll),
      s"Only the last member of a CompositeElasticSupport may be a catch all, got ${members.mkString(", ")}"
    )
  }

  private[elastic] def memberOf(
      members: List[CompositeMember],
      nodeId: String
  ): Option[CompositeMember] =
    members.find(_.owns(nodeId))

  private[elastic] def routed[A](
      members: List[CompositeMember],
      operation: String,
      nodeId: String,
      whenUnroutable: A
  )(delegate: CompositeMember => IO[A]): IO[A] =
    IO.defer {
      memberOf(members, nodeId) match {
        case Some(member) => delegate(member)
        case None =>
          IO(
            scribe.error(
              s"Composite elastic support: no member owns node $nodeId, skipping $operation. Members: ${members.map(_.name).mkString(", ")}"
            )
          ).as(whenUnroutable)
      }
    }.handleErrorWith { error =>
      IO(
        scribe.error(
          s"Composite elastic support: $operation failed for node $nodeId",
          error
        )
      ).as(whenUnroutable)
    }

  private def hostConfigOf(
      members: List[CompositeMember]
  ): TasksConfig => HostConfiguration =
    (config: TasksConfig) =>
      memberOf(members, config.nodeName)
        .flatMap(_.support.hostConfig)
        .map(_.apply(config))
        .getOrElse(tasks.hostConfigChosenFromConfig(config))
}

private class CompositeShutdownNode(members: List[CompositeMember])
    extends ShutdownNode {

  def shutdownRunningNode(nodeName: RunningJobId): IO[Unit] =
    CompositeElasticSupport.routed(
      members,
      "shutdownRunningNode",
      nodeName.value,
      ()
    )(_.support.shutdownFromNodeRegistry.shutdownRunningNode(nodeName))

  def shutdownPendingNode(nodeName: PendingJobId): IO[Unit] =
    CompositeElasticSupport.routed(
      members,
      "shutdownPendingNode",
      nodeName.value,
      ()
    )(_.support.shutdownFromNodeRegistry.shutdownPendingNode(nodeName))
}

private class CompositeShutdownSelfNode(members: List[CompositeMember])
    extends ShutdownSelfNode {

  def shutdownRunningNode(
      exitCode: Deferred[IO, ExitCode],
      nodeName: RunningJobId
  ): IO[Unit] =
    IO.defer {
      CompositeElasticSupport.memberOf(members, nodeName.value) match {
        case Some(member) =>
          member.support.shutdownFromWorker
            .shutdownRunningNode(exitCode, nodeName)
        case None =>
          IO(
            scribe.warn(
              s"Composite elastic support: no member owns this node ($nodeName), exiting the process instead of asking a backend to stop it."
            )
          ) *> exitCode.complete(ExitCode(0)).void
      }
    }.handleErrorWith { error =>
      IO(
        scribe.error(
          s"Composite elastic support: shutdownFromWorker failed for node ${nodeName.value}, exiting the process",
          error
        )
      ) *> exitCode.complete(ExitCode(0)).void
    }
}

private class CompositeConvertRunningToPending(members: List[CompositeMember])
    extends ConvertRunningToPending {

  override def convertRunningToPending(
      p: RunningJobId
  ): IO[Option[PendingJobId]] =
    CompositeElasticSupport.routed(
      members,
      "convertRunningToPending",
      p.value,
      Option.empty[PendingJobId]
    )(_.support.convertRunningToPending.convertRunningToPending(p))
}

private class CompositeGetNodeName(members: List[CompositeMember])
    extends GetNodeName {

  def getNodeName(config: TasksConfig): IO[RunningJobId] =
    if (config.nodeName.nonEmpty) IO.pure(RunningJobId(config.nodeName))
    else probe(members, config)

  private def probe(
      remaining: List[CompositeMember],
      config: TasksConfig
  ): IO[RunningJobId] = remaining match {
    case Nil =>
      IO(
        scribe.warn(
          s"Composite elastic support: no member recognized the name of this node, falling back to ${members.head.name}. Shutdown of this node from the registry may not work."
        )
      ) *> members.head.support.getNodeName.getNodeName(config)
    case member :: rest =>
      IO.defer(member.support.getNodeName.getNodeName(config)).attempt.flatMap {
        case Right(nodeName) if member.owns(nodeName.value) =>
          IO(
            scribe.info(
              s"Composite elastic support: this node belongs to member ${member.name} as $nodeName"
            )
          ).as(nodeName)
        case Right(_) => probe(rest, config)
        case Left(error) =>
          IO(
            scribe.warn(
              s"Composite elastic support: member ${member.name} failed to name this node",
              error
            )
          ) *> probe(rest, config)
      }
  }
}

private class CompositeCreateNodeFactory(members: List[CompositeMember])
    extends CreateNodeFactory {

  def apply(
      masterAddress: SimpleSocketAddress,
      masterPrefix: String,
      codeAddress: CodeAddress
  ): CreateNode =
    new CompositeCreateNode(
      members,
      members.map { member =>
        member.name -> member.support.createNodeFactory
          .apply(masterAddress, masterPrefix, codeAddress)
      }.toMap
    )
}

private class CompositeCreateNode(
    members: List[CompositeMember],
    createNodes: Map[String, CreateNode]
) extends CreateNode {

  def requestOneNewJobFromJobScheduler(
      request: ResourceRequest
  )(implicit
      taskConfig: TasksConfig
  ): IO[Either[String, (PendingJobId, ResourceAvailable)]] = {

    def attemptMembers(
        remaining: List[CompositeMember],
        errors: List[String]
    ): IO[Either[String, (PendingJobId, ResourceAvailable)]] = remaining match {
      case Nil =>
        IO.pure(
          Left(
            s"Every member of the composite elastic support failed to create a node for $request: ${errors.reverse.mkString("; ")}"
          )
        )
      case member :: rest =>
        IO.defer(
          createNodes(member.name).requestOneNewJobFromJobScheduler(request)
        ).attempt
          .flatMap {
            case Right(Right(created)) =>
              IO(
                scribe.info(
                  s"Composite elastic support: member ${member.name} created node ${created._1.value}"
                )
              ).as(Right(created))
            case Right(Left(error)) =>
              IO(
                scribe.info(
                  s"Composite elastic support: member ${member.name} could not create a node ($error)"
                )
              ) *> attemptMembers(rest, s"${member.name}: $error" :: errors)
            case Left(error) =>
              IO(
                scribe.error(
                  s"Composite elastic support: member ${member.name} failed to create a node",
                  error
                )
              ) *> attemptMembers(
                rest,
                s"${member.name}: ${error.getMessage}" :: errors
              )
          }
    }

    IO.defer {
      members.filter(_.accepts(request)) match {
        case Nil =>
          val message =
            s"No member of the composite elastic support accepts $request. Members: ${members.map(_.name).mkString(", ")}"
          IO(scribe.error(message)).as(Left(message))
        case candidates => attemptMembers(candidates, Nil)
      }
    }.handleErrorWith { error =>
      IO(
        scribe.error(
          s"Composite elastic support: node request failed for $request",
          error
        )
      ).as(Left(s"Composite elastic support failed: ${error.getMessage}"))
    }
  }

  override def convertRunningToPending(
      p: RunningJobId
  ): IO[Option[PendingJobId]] =
    CompositeElasticSupport.routed(
      members,
      "convertRunningToPending",
      p.value,
      Option.empty[PendingJobId]
    )(member => createNodes(member.name).convertRunningToPending(p))

  override def initializeNode(node: Node): IO[Unit] =
    CompositeElasticSupport.routed(
      members,
      "initializeNode",
      node.name.value,
      ()
    )(member => createNodes(member.name).initializeNode(node))
}
