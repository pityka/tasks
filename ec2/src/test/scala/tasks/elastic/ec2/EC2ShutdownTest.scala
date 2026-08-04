package tasks.elastic.ec2

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global
import tasks.shared.{PendingJobId, RunningJobId}

class EC2ShutdownTest extends AnyFunSuite with Matchers {

  private def registry(ids: String*): Ref[IO, Set[String]] =
    Ref.unsafe[IO, Set[String]](ids.toSet)

  test("running shutdown terminates the instance and drops it from the registry") {
    val ops = new FakeEC2Operations()
    val registered = registry("i-1")
    val shutdown = new EC2Shutdown(ops, registered)

    shutdown.shutdownRunningNode(RunningJobId("i-1")).unsafeRunSync()

    ops.terminated.get.unsafeRunSync() shouldBe List("i-1")
    registered.get.unsafeRunSync() shouldBe empty
  }

  test(
    "pending shutdown cancels the request and terminates the instance it left behind"
  ) {
    val ops = new FakeEC2Operations(
      spotRequestsById =
        Map("sfr-1" -> FakeEC2Operations.spotRequest("sfr-1", Some("i-1")))
    )
    val shutdown = new EC2Shutdown(ops, registry())

    shutdown.shutdownPendingNode(PendingJobId("sfr-1")).unsafeRunSync()

    ops.cancelled.get.unsafeRunSync() shouldBe List("sfr-1")
    ops.terminated.get.unsafeRunSync() shouldBe List("i-1")
  }

  test("pending shutdown cancels the request when it spawned no instance") {
    val ops = new FakeEC2Operations(
      spotRequestsById = Map("sfr-1" -> FakeEC2Operations.spotRequest("sfr-1"))
    )
    val shutdown = new EC2Shutdown(ops, registry())

    shutdown.shutdownPendingNode(PendingJobId("sfr-1")).unsafeRunSync()

    ops.cancelled.get.unsafeRunSync() shouldBe List("sfr-1")
    ops.terminated.get.unsafeRunSync() shouldBe empty
  }

  test(
    "pending shutdown does not terminate an instance which already registered as a worker"
  ) {
    val ops = new FakeEC2Operations(
      spotRequestsById =
        Map("sfr-1" -> FakeEC2Operations.spotRequest("sfr-1", Some("i-1")))
    )
    val shutdown = new EC2Shutdown(ops, registry("i-1"))

    shutdown.shutdownPendingNode(PendingJobId("sfr-1")).unsafeRunSync()

    ops.cancelled.get.unsafeRunSync() shouldBe List("sfr-1")
    ops.terminated.get.unsafeRunSync() shouldBe empty
  }

  test(
    "pending shutdown of a registered instance id touches nothing"
  ) {
    val ops = new FakeEC2Operations()
    val shutdown = new EC2Shutdown(ops, registry("i-1"))

    shutdown.shutdownPendingNode(PendingJobId("i-1")).unsafeRunSync()

    ops.cancelled.get.unsafeRunSync() shouldBe empty
    ops.terminated.get.unsafeRunSync() shouldBe empty
  }
}
