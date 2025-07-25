/*
 * The MIT License
 *
 * Copyright (c) 2018 Istvan Bartha
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

package tasks

import scala.util._

import tasks.elastic._
import tasks.shared._
import tasks.util.config._
import tasks.util.SimpleSocketAddress

import scala.concurrent.Future
import cats.effect.IO
import tasks.deploy.HostConfiguration
import cats.effect.kernel.Resource
import scala.collection.mutable.ArrayBuffer
import cats.effect.kernel.Ref
import cats.effect.FiberIO
import cats.effect.unsafe.implicits.global
import cats.effect.ExitCode
import cats.effect.kernel.Deferred

object JvmElasticSupport {

  case class State(
      taskSystems: List[
        (String, FiberIO[((TaskSystemComponents, HostConfiguration), IO[Unit])])
      ],
      nodesShutdown: List[String]
  )
  // val state =
  //   Ref.of[IO,State](State(Nil,Nil)).unsafeRunSync()

  class Shutdown(state: Ref[IO, State]) extends ShutdownNode {

    def shutdownRunningNode(nodeName: RunningJobId): IO[Unit] = {
      IO(scribe.info(s"Shutdown $nodeName")) *>
        state.flatModify { state =>
          val ts = state.taskSystems.filter(_._1 == nodeName.value)
          val newState = state.copy(
            nodesShutdown = state.nodesShutdown :+ nodeName.value,
            taskSystems = state.taskSystems.filterNot(_._1 == nodeName.value)
          )
          val release: IO[List[Unit]] = IO.parSequenceN(1)(
            ts.map(_._2.join.flatMap(_.embedError).flatMap(_._2))
          )
          (newState, release.void)
        }
    }

    def shutdownPendingNode(nodeName: PendingJobId): IO[Unit] =
      shutdownRunningNode(RunningJobId(nodeName.value))

  }
  class ShutdownSelf(state: Ref[IO, State]) extends ShutdownSelfNode {

    def shutdownRunningNode(
        exitCode: Deferred[IO, ExitCode],
        nodeName: RunningJobId
    ): IO[Unit] =
      (new Shutdown(state)).shutdownRunningNode(nodeName)

  }

  class JvmCreateNode(
      state: Ref[IO, State],
      externalQueueState: Option[Ref[IO, tasks.queue.QueueImpl.State]],
      masterAddress: SimpleSocketAddress,
      masterPrefix: String
  ) extends CreateNode {

    def requestOneNewJobFromJobScheduler(
        requestSize: tasks.shared.ResourceRequest
    )(implicit
        config: TasksConfig
    ): IO[Either[String, (PendingJobId, ResourceAvailable)]] = {
      val jobid =
        java.util.UUID.randomUUID.toString.replace("-", "")

      val ts = {

        defaultTaskSystem(
          config = s"""
    
    hosts.master = "${masterAddress.getHostName}:${masterAddress.getPort}"
    hosts.masterprefix = ${masterPrefix}
    hosts.app = false
    tasks.disableRemoting = false
    tasks.elastic.nodename = $jobid
    tasks.addShutdownHook = false 
    tasks.fileservice.storageURI="${config.storageURI.toString}"
    """,
          s3Client = Resource.pure(None),
          elasticSupport =
            JvmGrid.make(externalQueueState).map(v => Some(v._2)),
          externalQueueState = Resource.pure(
            externalQueueState.map(ref => tasks.util.Transaction.fromRef(ref))
          )
        ).allocated.start
      }
      ts.flatMap { ts =>
        state.update(state =>
          state.copy(taskSystems = state.taskSystems :+ ((jobid, ts)))
        )
      }.map { _ =>
        Right(
          (
            PendingJobId(jobid),
            ResourceAvailable(
              cpu = requestSize.cpu._1,
              memory = requestSize.memory,
              scratch = requestSize.scratch,
              gpu = 0 until requestSize.gpu toList,
              image = None
            )
          )
        )
      }

    }

  }

  class JvmCreateNodeFactory(
      ref: Ref[IO, State],
      externalQueueState: Option[Ref[IO, tasks.queue.QueueImpl.State]]
  ) extends CreateNodeFactory {
    def apply(
        master: SimpleSocketAddress,
        masterPrefix: String,
        codeAddress: CodeAddress
    ) =
      new JvmCreateNode(ref, externalQueueState, master, masterPrefix)
  }

  object JvmGetNodeName extends GetNodeName {
    def getNodeName(config: TasksConfig) =
      IO.pure(RunningJobId(config.nodeName))
  }

  class JvmNodeControl(
      state: Ref[IO, State]
  ) {
    def list = state.get.map(_.taskSystems.map(_._1))
    def stop(a: String) = {
      scribe.info(s"Stop called on $a")
      (new Shutdown(state)).shutdownRunningNode(RunningJobId(a))
    }

  }

  object JvmGrid {

    val stateResource = Resource.make(
      Ref.of[IO, State](State(Nil, Nil))
    ) { state =>
      state.get.flatMap { state =>
        IO.parSequenceN(1)(
          state.taskSystems.map(_._2.join.flatMap(_.embedError.flatMap(_._2)))
        ).void
      }
    }

    def make(
        externalQueueState: Option[Ref[IO, tasks.queue.QueueImpl.State]]
    ): Resource[IO, (JvmNodeControl, ElasticSupport)] =
      stateResource.map { state =>
        val controller = new JvmNodeControl(state)

        controller ->
          new ElasticSupport(
            hostConfig = None,
            shutdownFromNodeRegistry = new Shutdown(state),
            shutdownFromWorker = new ShutdownSelf(state),
            createNodeFactory =
              new JvmCreateNodeFactory(state, externalQueueState),
            getNodeName = JvmGetNodeName
          )

      }
  }
}
