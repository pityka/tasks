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

import tasks.queue.{QueueActor, LauncherActor}
import tasks.shared.monitor._
import tasks.shared._
import tasks.util.config._
import tasks.wire._
import scala.util.{Failure, Success}
import tasks.util.Messenger
import tasks.util.message.Message
import tasks.util.message.MessageData
import tasks.util.Actor
import cats.effect.unsafe.implicits.global
import tasks.util.message.Address
import cats.effect.IO
import cats.effect.kernel.Ref
import tasks.util.SimpleSocketAddress
import cats.effect.kernel.Resource
import tasks.util.Ask

case class RemoteNodeRegistry(address: Address)

object NodeRegistry {

  def makeReference(
      masterAddress: SimpleSocketAddress,
      messenger: Messenger,
      elasticSupport: Option[ElasticSupport]
  )(implicit config: TasksConfig): IO[RemoteNodeRegistry] =
    Ask
      .ask(
        target = NodeRegistry.address,
        data = MessageData.Ping,
        timeout = config.pendingNodeTimeout,
        messenger = messenger
      )
      .map {
        case Right(Some(_)) => (Right(RemoteNodeRegistry(NodeRegistry.address)))
        case Right(None) => (
          Left(new RuntimeException(s"NodeRegistry not reachable"))
        )
        case Left(e) => Left(e)
      }
      .flatMap {
        case Right(v) => IO.pure(v)
        case Left(e) =>
          IO(elasticSupport.get.selfShutdownNow()) *> IO.raiseError(
            new RuntimeException("Remote node registry not found", e)
          )
      }

  sealed trait Event
  case object NodeRequested extends Event
  case class NodeIsPending(
      pendingJobId: PendingJobId,
      resource: ResourceAvailable
  ) extends Event
  case class NodeIsUp(node: Node, pendingJobId: PendingJobId) extends Event
  case class NodeIsDown(node: Node) extends Event
  case class InitFailed(pending: PendingJobId) extends Event

  case class State(
      running: Map[RunningJobId, ResourceAvailable],
      pending: Map[PendingJobId, ResourceAvailable],
      cumulativeRequested: Int
  ) {
    def update(e: Event): State = {
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
  val address: Address = Address("NodeRegistry")
}

class NodeRegistry(
    unmanagedResource: ResourceAvailable,
    createNode: CreateNode,
    decideNewNode: DecideNewNode,
    shutdownNode: ShutdownNode,
    targetQueue: QueueActor,
    messenger: Messenger
)(implicit config: TasksConfig)
    extends Actor.ActorBehavior[NodeRegistry.State, Unit](messenger) {

  import NodeRegistry._
  val address = NodeRegistry.address
  val init = NodeRegistry.State.empty
  def derive(ref: Ref[IO, State]): Unit = ()
  override def schedulers(
      ref: Ref[IO, NodeRegistry.State]
  ): Option[IO[fs2.Stream[IO, Unit]]] = Some(
    IO.pure(
      (fs2.Stream.sleep[IO](config.queueCheckInitialDelay) ++ fs2.Stream
        .fixedRate[IO](config.queueCheckInterval))
        .evalMap { _ =>
          messenger.submit(
            Message(
              from = address,
              to = targetQueue.address,
              data = MessageData.HowLoadedAreYou
            )
          )
        }
    )
  )

  override def release(st: State): IO[Unit] = {
    IO.parSequenceN(1)(
      (st.running.map { case (node, _) =>
        IO {
          scribe.info("Shutting down node " + node)
          shutdownNode.shutdownRunningNode(node)
        }
      } ++
        st.pending.map { case (node, _) =>
          IO {
            shutdownNode.shutdownPendingNode(node)
          }
        }).toList
    ).void
  }

  def receive = (state, ref) => {

    case Message(queueStat: MessageData.QueueStat, from, _) =>
      val logIO = if (config.logQueueStatus) {
        IO {
          scribe.info(
            s"Queued tasks: ${queueStat.queued.size}. Running tasks: ${queueStat.running.size}. Pending nodes: ${state.pending.size} . Running nodes: ${state.running.size}. Largest request: ${queueStat.queued
                .sortBy(_._2.cpu)
                .lastOption}/${queueStat.queued.sortBy(_._2.memory).lastOption}"
          )
        }
      } else IO.unit
      val (newState, io) =
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
              state -> IO(
                scribe.info(
                  "New node request will not proceed: pending nodes or reached max nodes. max: " + config.maxNodes + ", pending: " + state.pending.size + ", running: " + state.running.size
                )
              )
            } else {

              val allowedNewNodes = math.min(
                config.maxNodes - (state.running.size + state.pending.size),
                config.maxNodesCumulative - state.cumulativeRequested
              )

              val requestedNodes = neededNodes.take(allowedNewNodes)

              val updatedState: IO[Unit] = IO(
                scribe.info(
                  "Request " + requestedNodes.size + " node. One from each: " + requestedNodes.keySet
                )
              ) *> IO
                .parSequenceN(1)(requestedNodes.toList.map {
                  case (request, _) =>
                    IO(createNode.requestOneNewJobFromJobScheduler(request))
                      .flatMap {
                        case Failure(e) =>
                          IO(
                            scribe.warn(
                              "Request failed: " + e.getMessage + " " + e
                            )
                          ) *>
                            ref.update(_.update(NodeRequested))
                        case Success((jobId, size)) =>
                          IO(
                            scribe.info(
                              s"Request succeeded. Job id: $jobId, size: $size"
                            )
                          ) *> ref.update(
                            _.update(NodeRequested)
                              .update(NodeIsPending(jobId, size))
                          ) *> fs2.Stream
                            .sleep[IO](config.pendingNodeTimeout)
                            .evalMap { initFailed =>
                              ref.flatModify { state =>
                                if (state.pending.contains(jobId)) {
                                  scribe.warn("Node init failed: " + jobId)

                                  state.update(InitFailed(jobId)) ->
                                    IO(shutdownNode.shutdownPendingNode(jobId))

                                } else (state, IO.unit)
                              }
                            }
                            .compile
                            .drain
                            .start
                            .void

                      }
                })
                .void

              (state, updatedState)

            }
          } else (state, IO.unit)

        } catch {
          case e: Exception =>
            (state, IO(scribe.error(e, "Error during requesting node")))
        }

      newState -> logIO *> io

    case Message(MessageData.NodeComingUp(node), from, _) =>
      val Node(jobId, _, _) = node

      createNode.convertRunningToPending(jobId) match {
        case Some(convertedRunningId) =>
          val newState = state.update(NodeIsUp(node, convertedRunningId))

          val io = IO(createNode.initializeNode(node)) *>
            tasks.util.Actor
              .makeFromBehavior(
                new NodeKiller(
                  shutdownNode = shutdownNode,
                  targetLauncherActor = node.launcherActor,
                  targetNode = node,
                  listener = address,
                  messenger = messenger
                ),
                messenger
              )
              .allocated
              .void

          (newState -> io)

        case None =>
          state -> IO(
            scribe.error(
              s"Failed to find running job id from pending job id. $node"
            )
          )
      }

    case Message(MessageData.RemoveNode(node), from, _) =>
      state.update(NodeIsDown(node)) -> IO.unit
    case Message(MessageData.Ping, from, _) =>
      state -> sendTo(from, MessageData.Ping)

  }

}
