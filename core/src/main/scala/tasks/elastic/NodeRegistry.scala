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
import scala.util.Failure

class NodeRegistry(
    unmanagedResource: CPUMemoryAvailable,
    createNode: CreateNode,
    decideNewNode: DecideNewNode,
    shutdownNode: ShutdownNode,
    targetQueue: QueueActor
)(implicit config: TasksConfig)
    extends Actor
    with ActorLogging {

  private val jobregistry =
    scala.collection.mutable.Set[Tuple2[RunningJobId, CPUMemoryAvailable]]()

  private val pending =
    scala.collection.mutable.Set[Tuple2[PendingJobId, CPUMemoryAvailable]]()

  private var allTime = 0

  private def toPend(p: PendingJobId, size: CPUMemoryAvailable) {
    pending += ((p, size))
  }

  def allRegisteredNodes =
    Set[Tuple2[RunningJobId, CPUMemoryAvailable]](jobregistry.toSeq: _*)

  def pendingNodes = {
    Set[Tuple2[PendingJobId, CPUMemoryAvailable]](pending.toSeq: _*)
  }

  def requestNewNodes(types: Map[CPUMemoryRequest, Int]) = {
    if (types.values.sum > 0) {
      if (config.maxNodes > (jobregistry.size + pending.size) &&
          allTime <= config.maxNodesCumulative) {

        log.info(
          "Request " + types.size + " node. One from each: " + types.keySet)

        types.foreach {
          case (request, _) =>
            val jobinfo = createNode.requestOneNewJobFromJobScheduler(request)
            allTime += 1

            jobinfo match {
              case Failure(e) =>
                log.warning("Request failed: " + e.getMessage + " " + e)
              case _ => ()
            }

            jobinfo.foreach { ji =>
              val jobid = ji._1
              val size = ji._2
              toPend(jobid, size)
            }
        }

      } else {
        log.info(
          "New node request will not proceed: pending nodes or reached max nodes. max: " + config.maxNodes + ", pending: " + pending.size + ", running: " + jobregistry.size)
      }
    }
  }

  def refreshPendingList: List[PendingJobId] = pending.toList.map(_._1)

  private def registerJob(id: RunningJobId, size: CPUMemoryAvailable) {
    val elem = (id, size)
    jobregistry += elem
    val pendingID = createNode.convertRunningToPending(id)
    if (pendingID.isDefined) {
      scala.util.Try {
        pending -= (pending.filter(_._1 == (pendingID.get)).head)
      }
    } else {
      val activePendings = refreshPendingList
      val removal =
        pending.toSeq.map(_._1).filter(x => !activePendings.contains(x))
      removal.foreach { r =>
        pending -= (pending.filter(_._1 == r).head)
      }
    }

    log.debug(s"registerJob: $id , $size . ")
  }

  def registerNode(node: Node) {
    log.debug("Registering node: " + node)
    val jobid = node.name
    val size = node.size
    registerJob(jobid, size)
    createNode.initializeNode(node)
    context.actorOf(
      Props(
        new NodeKiller(shutdownNode = shutdownNode,
                       targetLauncherActor = LauncherActor(node.launcherActor),
                       targetNode = node))
        .withDispatcher("my-pinned-dispatcher"),
      "nodekiller" + node.name.value.replace("://", "___")
    )
  }

  def deregisterNode(n: Node) {
    jobregistry -= ((n.name, n.size))
  }

  def initfailed(pendingID: PendingJobId) {
    (pending.filter(_._1 == (pendingID)).headOption).foreach { x =>
      pending -= x
    }
  }

  private var scheduler: Cancellable = null

  override def preStart {
    log.info("NodeCreator start. Monitoring actor: " + targetQueue)

    import context.dispatcher

    scheduler = context.system.scheduler.schedule(
      initialDelay = config.queueCheckInitialDelay,
      interval = config.queueCheckInterval,
      receiver = self,
      message = MeasureTime
    )

    context.system.eventStream.subscribe(self, classOf[NodeIsDown])

  }

  override def postStop {
    scheduler.cancel
    log.info("NodeCreator stopping.")
    allRegisteredNodes.foreach { node =>
      log.info("Shutting down node " + node)
      shutdownNode.shutdownRunningNode(node._1)
    }
    pendingNodes.foreach { node =>
      shutdownNode.shutdownPendingNode(node._1)
    }
    log.info("Shutted down all registered nodes.")
  }

  def startNewNode(types: Map[CPUMemoryRequest, Int]) {
    requestNewNodes(types)
  }

  def receive = {
    case MeasureTime =>
      log.debug("Tick from scheduler.")

      targetQueue.actor ! HowLoadedAreYou

    case m: QueueStat =>
      if (config.logQueueStatus) {
        log.info(
          s"Queued tasks: ${m.queued.size}. Running tasks: ${m.running.size}. Pending nodes: ${pendingNodes.size} . Running nodes: ${allRegisteredNodes.size}. Largest request: ${m.queued
            .sortBy(_._2.cpu)
            .lastOption}/${m.queued.sortBy(_._2.memory).lastOption}")
      }
      try {
        startNewNode(
          decideNewNode.needNewNode(
            m,
            allRegisteredNodes.toSeq.map(_._2) ++ Seq(unmanagedResource),
            pendingNodes.toSeq.map(_._2)))
      } catch {
        case e: Exception => log.error(e, "Error during requesting node")
      }

    case NodeComingUp(node) =>
      log.info("NodeComingUp: " + node)
      try {
        registerNode(node)
      } catch {
        case e: Exception => log.error(e, "unexpected exception")
      }

    case NodeIsDown(node) =>
      log.debug("NodeIsDown: " + node)
      try {
        deregisterNode(node)
      } catch {
        case e: Exception => log.error(e, "unexpected exception")
      }

    case InitFailed(pending) =>
      log.error("Node init failed: " + pending)
      try {
        initfailed(pending)
        shutdownNode.shutdownPendingNode(pending)
      } catch {
        case e: Exception => log.error(e, "unexpected exception")
      }

  }

}
