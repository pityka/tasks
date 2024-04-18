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

package tasks.elastic

import akka.actor.{Actor, Cancellable, ActorLogging, Props}

import tasks.queue.{QueueActor, LauncherActor}
import tasks.shared.monitor._
import tasks.shared._
import tasks.util.config._
import tasks.wire._
import scala.util.{Failure, Success}
import tasks.ui.EventListener

object NodeRegistry {

  sealed trait Event
  case object NodeRequested extends Event
  case class NodeIsPending(
      pendingJobId: PendingJobId,
      resource: ResourceAvailable
  ) extends Event
  case class NodeIsUp(node: Node, pendingJobId: PendingJobId) extends Event
  case class NodeIsDown(node: Node) extends Event
  case class InitFailed(pending: PendingJobId) extends Event

}

class NodeRegistry(
    unmanagedResource: ResourceAvailable,
    createNode: CreateNode,
    decideNewNode: DecideNewNode,
    shutdownNode: ShutdownNode,
    targetQueue: QueueActor,
    eventListener: Option[EventListener[NodeRegistry.Event]]
)(implicit config: TasksConfig)
    extends Actor
    with ActorLogging {

  import NodeRegistry._

  case class State(
      running: Map[RunningJobId, ResourceAvailable],
      pending: Map[PendingJobId, ResourceAvailable],
      cumulativeRequested: Int
  ) {
    def update(e: Event): State = {
      eventListener.foreach(_.receive(e))
      e match {
        case NodeRequested =>
          copy(cumulativeRequested = cumulativeRequested + 1)
        case NodeIsUp(Node(runningJobId, resource, _), pendingJobId) =>
          copy(
            pending = pending - pendingJobId,
            running = running + ((runningJobId, resource))
          )
        case NodeIsDown(Node(runningJobId, _, _)) =>
          copy(running = running - runningJobId)
        case InitFailed(pendingJobId) => copy(pending = pending - pendingJobId)
        case NodeIsPending(pendingJobId, resource) =>
          copy(pending = pending + ((pendingJobId, resource)))
      }
    }
  }

  object State {
    val empty = State(Map(), Map(), 0)
  }

  private var scheduler: Cancellable = null

  var currentState: State = State.empty

  def become(state: State) = {
    currentState = state
    context.become(running(state))
  }

  def receive = { case _ =>
    ???
  }

  override def preStart(): Unit = {
    log.info("NodeCreator start. Monitoring actor: " + targetQueue)

    import context.dispatcher

    scheduler = context.system.scheduler.scheduleAtFixedRate(
      initialDelay = config.queueCheckInitialDelay,
      interval = config.queueCheckInterval,
      receiver = self,
      message = MeasureTime
    )

    become(State.empty)

  }

  override def postStop(): Unit = {
    scheduler.cancel()
    log.info("NodeCreator stopping.")
    currentState.running.foreach { case (node, _) =>
      log.info("Shutting down node " + node)
      shutdownNode.shutdownRunningNode(node)
    }
    currentState.pending.foreach { case (node, _) =>
      shutdownNode.shutdownPendingNode(node)
    }
    log.info("Shutted down all registered nodes.")
  }

  def running(state: State): Receive = {
    case MeasureTime =>
      log.debug("Tick from scheduler.")
      targetQueue.actor ! HowLoadedAreYou

    case queueStat: QueueStat =>
      if (config.logQueueStatus) {
        log.info(
          s"Queued tasks: ${queueStat.queued.size}. Running tasks: ${queueStat.running.size}. Pending nodes: ${state.pending.size} . Running nodes: ${state.running.size}. Largest request: ${queueStat.queued
              .sortBy(_._2.cpu)
              .lastOption}/${queueStat.queued.sortBy(_._2.memory).lastOption}"
        )
      }
      try {
        val neededNodes = decideNewNode.needNewNode(
          queueStat,
          state.running.toSeq.map(_._2) ++ Seq(unmanagedResource),
          state.pending.toSeq.map(_._2)
        )

        val skip = neededNodes.values.sum == 0
        if (!skip) {
          val canRequest =
            config.maxNodes > (state.running.size + state.pending.size) &&
              state.cumulativeRequested <= config.maxNodesCumulative
          if (!canRequest) {
            log.info(
              "New node request will not proceed: pending nodes or reached max nodes. max: " + config.maxNodes + ", pending: " + state.pending.size + ", running: " + state.running.size
            )
          } else {

            val allowedNewNodes = math.min(
              config.maxNodes - (state.running.size + state.pending.size),
              config.maxNodesCumulative - state.cumulativeRequested
            )

            val requestedNodes = neededNodes.take(allowedNewNodes)

            log.info(
              "Request " + requestedNodes.size + " node. One from each: " + requestedNodes.keySet
            )

            val updatedState = requestedNodes.foldLeft(state) {
              case (state, (request, _)) =>
                val jobinfo =
                  createNode.requestOneNewJobFromJobScheduler(request)
                val withRequested = state.update(NodeRequested)

                jobinfo match {
                  case Failure(e) =>
                    log.warning("Request failed: " + e.getMessage + " " + e)
                    withRequested
                  case Success((jobId, size)) =>
                    log.info(s"Request succeeded. Job id: $jobId, size: $size")
                    context.system.scheduler.scheduleOnce(
                      delay = config.pendingNodeTimeout,
                      receiver = self,
                      message = InitFailed(jobId)
                    )(context.dispatcher)
                    withRequested.update(NodeIsPending(jobId, size))

                }
            }
            become(updatedState)

          }
        }

      } catch {
        case e: Exception => log.error(e, "Error during requesting node")
      }

    case NodeComingUp(node) =>
      log.info("NodeComingUp: " + node)
      try {
        log.debug("Registering node: " + node)
        val Node(jobId, resource, _) = node

        createNode.convertRunningToPending(jobId) match {
          case Some(convertedRunningId) =>
            val newState = state.update(NodeIsUp(node, convertedRunningId))
            become(newState)

            log.debug(s"registerJob: $jobId , $resource . ")

            createNode.initializeNode(node)
            context.actorOf(
              Props(
                new NodeKiller(
                  shutdownNode = shutdownNode,
                  targetLauncherActor = LauncherActor(node.launcherActor),
                  targetNode = node,
                  listener = self
                )
              ).withDispatcher("nodekiller-pinned")
            )
          case None =>
            log.error(
              s"Failed to find running job id from pending job id. $node"
            )
        }

      } catch {
        case e: Exception => log.error(e, "unexpected exception")
      }

    case RemoveNode(node) =>
      log.info("RemoveNode: " + node)
      try {
        become(state.update(NodeIsDown(node)))
      } catch {
        case e: Exception => log.error(e, "unexpected exception")
      }
    case InitFailed(pending) =>
      if (state.pending.contains(pending)) {
        log.warning("Node init failed: " + pending)
        try {
          become(state.update(InitFailed(pending)))
          shutdownNode.shutdownPendingNode(pending)
        } catch {
          case e: Exception => log.error(e, "unexpected exception")
        }
      }
  }

}
