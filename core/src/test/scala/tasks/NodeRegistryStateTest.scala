package tasks

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global

import tasks.elastic.NodeRegistryState
import tasks.elastic.NodeRegistryState._
import tasks.shared._

import scala.concurrent.duration._

class NodeRegistryStateTest extends AnyFunSuite with Matchers {

  private val shapeA = ResourceAvailable(1, 100, 0, Nil, None)
  private val shapeB = ResourceAvailable(4, 500, 0, Nil, None)

  test("NodeIsPending removes the matching committed shape, not the head") {
    val s0 = NodeRegistryState.State.empty
      .update(NodeRequested(shapeA))
      .update(NodeRequested(shapeB))

    s0.inFlightRequests should contain theSameElementsAs List(shapeA, shapeB)

    val s1 = s0.update(
      NodeIsPending(
        PendingJobId("job-a"),
        resource = shapeA.copy(cpu = 2),
        committedResource = shapeA
      )
    )

    s1.inFlightRequests shouldBe List(shapeB)
    s1.pending.keySet shouldBe Set(PendingJobId("job-a"))
  }

  test("NodeRequestFailed removes the matching committed shape") {
    val s0 = NodeRegistryState.State.empty
      .update(NodeRequested(shapeA))
      .update(NodeRequested(shapeB))
      .update(NodeRequested(shapeA))

    val s1 = s0.update(NodeRequestFailed(shapeB))

    s1.inFlightRequests.count(_ == shapeA) shouldBe 2
    s1.inFlightRequests.count(_ == shapeB) shouldBe 0
  }

  test("removeFirst with duplicate shapes removes one occurrence only") {
    val s0 = NodeRegistryState.State.empty
      .update(NodeRequested(shapeA))
      .update(NodeRequested(shapeA))
      .update(NodeRequested(shapeB))

    val s1 = s0.update(
      NodeIsPending(PendingJobId("j1"), shapeA, committedResource = shapeA)
    )

    s1.inFlightRequests.count(_ == shapeA) shouldBe 1
    s1.inFlightRequests.count(_ == shapeB) shouldBe 1
  }

  test("AllStop clears inFlightRequests") {
    val s0 = NodeRegistryState.State.empty
      .update(NodeRequested(shapeA))
      .update(NodeRequested(shapeB))
      .update(AllStop)

    s0.inFlightRequests shouldBe Nil
  }

  test(
    "failed in-flight frees the shape so it can be re-requested"
  ) {
    val s0 = NodeRegistryState.State.empty.update(NodeRequested(shapeA))
    s0.inFlightRequests shouldBe List(shapeA)
    s0.cumulativeRequested shouldBe 1

    val s1 = s0.update(NodeRequestFailed(shapeA))
    s1.inFlightRequests shouldBe Nil
    s1.cumulativeRequested shouldBe 1

    val s2 = s1.update(NodeRequested(shapeA))
    s2.inFlightRequests shouldBe List(shapeA)
    s2.cumulativeRequested shouldBe 2
  }

  test(
    "removeFirst is idempotent when the shape is not present"
  ) {
    val s0 = NodeRegistryState.State.empty.update(NodeRequested(shapeB))
    val s1 = s0.update(NodeRequestFailed(shapeA))
    s1.inFlightRequests shouldBe List(shapeB)
  }

  test(
    "cancelled in-flight IO releases its pre-committed slot via guaranteeCase"
  ) {
    val program = for {
      ref <- Ref.of[IO, NodeRegistryState.State](
        NodeRegistryState.State.empty.update(NodeRequested(shapeA))
      )
      committedResource = shapeA
      recordFailure = ref.update(_.update(NodeRequestFailed(committedResource)))
      hangingRequest = IO
        .uncancelable { poll =>
          poll(IO.never[Either[String, (PendingJobId, ResourceAvailable)]])
            .flatMap {
              case Left(_)  => recordFailure
              case Right(_) => IO.unit
            }
        }
        .guaranteeCase {
          case cats.effect.Outcome.Canceled() => recordFailure
          case _                              => IO.unit
        }
      fiber <- hangingRequest.start
      _ <- IO.sleep(50.millis)
      _ <- fiber.cancel
      st <- ref.get
    } yield st

    val st = program.unsafeRunSync()
    st.inFlightRequests shouldBe Nil
    st.cumulativeRequested shouldBe 1
  }

  test(
    "cumulativeRequested is monotonic and not decremented on fail/pending"
  ) {
    val s = NodeRegistryState.State.empty
      .update(NodeRequested(shapeA))
      .update(NodeRequested(shapeB))
      .update(NodeRequestFailed(shapeA))
      .update(
        NodeIsPending(PendingJobId("j"), shapeB, committedResource = shapeB)
      )

    s.cumulativeRequested shouldBe 2
    s.inFlightRequests shouldBe Nil
  }
}
