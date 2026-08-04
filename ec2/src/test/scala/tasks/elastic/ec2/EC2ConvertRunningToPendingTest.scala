package tasks.elastic.ec2

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import cats.effect.unsafe.implicits.global
import tasks.shared.{PendingJobId, RunningJobId}

class EC2ConvertRunningToPendingTest extends AnyFunSuite with Matchers {

  test("resolves the spot request id which spawned the reported instance") {
    val ops = new FakeEC2Operations(
      spotRequestsByInstance =
        Map("i-1" -> FakeEC2Operations.spotRequest("sfr-1", Some("i-1")))
    )

    val result = new EC2ConvertRunningToPending(ops)
      .convertRunningToPending(RunningJobId("i-1"))
      .unsafeRunSync()

    result shouldBe Some(PendingJobId("sfr-1"))
  }

  test("falls back to the instance id when no spot request is found") {
    val ops = new FakeEC2Operations()

    val result = new EC2ConvertRunningToPending(ops)
      .convertRunningToPending(RunningJobId("i-1"))
      .unsafeRunSync()

    result shouldBe Some(PendingJobId("i-1"))
  }

  test("falls back to the instance id when the lookup fails") {
    val ops = new FakeEC2Operations(
      lookupFailure = Some(new RuntimeException("throttled"))
    )

    val result = new EC2ConvertRunningToPending(ops)
      .convertRunningToPending(RunningJobId("i-1"))
      .unsafeRunSync()

    result shouldBe Some(PendingJobId("i-1"))
  }

  test("falls back to the instance id when the spot request has no id") {
    val ops = new FakeEC2Operations(
      spotRequestsByInstance =
        Map("i-1" -> com.amazonaws.ec2.SpotInstanceRequest())
    )

    val result = new EC2ConvertRunningToPending(ops)
      .convertRunningToPending(RunningJobId("i-1"))
      .unsafeRunSync()

    result shouldBe Some(PendingJobId("i-1"))
  }
}
