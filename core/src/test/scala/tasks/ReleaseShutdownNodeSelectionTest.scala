package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global

import scala.concurrent.duration._

import tasks.elastic.NodeRegistryState
import tasks.elastic.ShutdownNode
import tasks.queue.QueueImpl
import tasks.shared.{PendingJobId, ResourceAvailable, RunningJobId}
import tasks.util.{LocalMessenger, Transaction}
import tasks.util.message.{LauncherName, Node}

class ReleaseShutdownNodeSelectionTest extends FunSuite with Matchers {

  implicit val config: tasks.util.config.TasksConfig =
    tasks.util.config.parse(() => org.ekrich.config.ConfigFactory.load())

  private class RecordingShutdownNode(calls: Ref[IO, List[String]])
      extends ShutdownNode {
    def shutdownRunningNode(nodeName: RunningJobId): IO[Unit] =
      calls.update(_ :+ s"running:${nodeName.value}")
    def shutdownPendingNode(nodeName: PendingJobId): IO[Unit] =
      calls.update(_ :+ s"pending:${nodeName.value}")
  }

  private val shape =
    ResourceAvailable(
      cpu = 1,
      memory = 100,
      scratch = 0,
      gpu = Nil,
      image = None
    )

  private val runningId = RunningJobId("running-1")
  private val pendingSeedId = PendingJobId("pending-seed")
  private val pendingOrphanId = PendingJobId("pending-orphan")

  private val seededNodes = NodeRegistryState.State.empty
    .update(NodeRegistryState.NodeRequested(shape))
    .update(NodeRegistryState.NodeIsPending(pendingSeedId, shape, shape))
    .update(
      NodeRegistryState.NodeIsUp(
        Node(runningId, shape, LauncherName("launcher-1")),
        pendingSeedId
      )
    )
    .update(NodeRegistryState.NodeRequested(shape))
    .update(NodeRegistryState.NodeIsPending(pendingOrphanId, shape, shape))

  private val seededState =
    QueueImpl.State.empty.copy(nodes = seededNodes)

  private def makeQueue(
      transaction: Transaction[QueueImpl.State],
      shutdown: ShutdownNode,
      session: String
  ) = LocalMessenger.make.flatMap { messenger =>
    QueueImpl.fromTransaction(
      transaction = transaction,
      cache = null,
      messenger = messenger,
      shutdownNode = Some(shutdown),
      decideNewNode = None,
      createNode = None,
      convertRunningToPending = None,
      unmanagedResource = ResourceAvailable.empty,
      meterProvider = org.typelevel.otel4s.metrics.MeterProvider.noop[IO],
      mainProcessSession = Some(session)
    )
  }

  test(
    "release does not shut down any node when another main process is still using the shared queue state"
  ) {
    val program = for {
      stateRef <- Ref.of[IO, QueueImpl.State](seededState)
      callsA <- Ref.of[IO, List[String]](Nil)
      callsB <- Ref.of[IO, List[String]](Nil)
      transaction = Transaction.fromRef(stateRef)
      shutdownA = new RecordingShutdownNode(callsA)
      shutdownB = new RecordingShutdownNode(callsB)
      observed <- makeQueue(transaction, shutdownA, "session-A").use { _ =>
        for {
          bothUp <- makeQueue(transaction, shutdownB, "session-B").use { _ =>
            stateRef.get
          }
          callsAfterB <- callsB.get
          stateAfterBLeft <- stateRef.get
        } yield (bothUp, callsAfterB, stateAfterBLeft)
      }
      (bothUp, callsAfterB, stateAfterBLeft) = observed
      callsAfterAll <- callsA.get
      stateAfterAll <- stateRef.get
    } yield (bothUp, callsAfterB, stateAfterBLeft, callsAfterAll, stateAfterAll)

    val (bothUp, callsAfterB, stateAfterBLeft, callsAfterAll, stateAfterAll) =
      program
        .unsafeRunTimed(60.seconds)
        .getOrElse(throw new RuntimeException("timeout"))

    bothUp.mainProcesses shouldBe Set("session-A", "session-B")

    callsAfterB shouldBe empty
    stateAfterBLeft.mainProcesses shouldBe Set("session-A")
    stateAfterBLeft.nodes.running.keySet shouldBe Set(runningId)
    stateAfterBLeft.nodes.pending.keySet shouldBe Set(pendingOrphanId)

    callsAfterAll.toSet shouldBe Set(
      s"running:${runningId.value}",
      s"pending:${pendingOrphanId.value}"
    )

    stateAfterAll shouldBe QueueImpl.State.empty
  }

  test(
    "a single main process still shuts down every running and pending node on release"
  ) {
    val program = for {
      stateRef <- Ref.of[IO, QueueImpl.State](seededState)
      calls <- Ref.of[IO, List[String]](Nil)
      transaction = Transaction.fromRef(stateRef)
      shutdown = new RecordingShutdownNode(calls)
      _ <- makeQueue(transaction, shutdown, "session-only").use(_ => IO.unit)
      afterRelease <- calls.get
    } yield afterRelease

    val afterRelease = program
      .unsafeRunTimed(60.seconds)
      .getOrElse(throw new RuntimeException("timeout"))

    afterRelease.toSet shouldBe Set(
      s"running:${runningId.value}",
      s"pending:${pendingOrphanId.value}"
    )
  }

}
