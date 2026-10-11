package tasks.elastic.batch

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BatchJobDefinitionResolutionTest extends AnyFunSuite with Matchers {

  private val minimal =
    BatchConfig(jobDefinition = "default-jd", queues = List("default-queue"))

  test("the short constructor fills in the defaults") {
    minimal.region shouldBe None
    minimal.jobQueue shouldBe None
    minimal.minimumCpu shouldBe 1
    minimal.minimumMemory shouldBe 512
    minimal.logGroup shouldBe "/aws/batch/job"
    minimal.jobDefinitionsByImage shouldBe empty
    minimal.tags shouldBe empty
  }

  test("an empty jobDefinition is rejected") {
    an[IllegalArgumentException] should be thrownBy minimal.copy(
      jobDefinition = ""
    )
  }

  test("an empty queue name is rejected") {
    an[IllegalArgumentException] should be thrownBy minimal.withQueues("")
  }

  test("resolveJobDefinition(None) returns the default jobDefinition") {
    minimal.resolveJobDefinition(None) shouldBe Right("default-jd")
  }

  test("resolveJobDefinition(Some(x)) returns the mapped job definition") {
    val c = minimal
      .withJobDefinitionForImage("my-image:v1", "my-jd-v1")
      .withJobDefinitionForImage("my-image:v2", "my-jd-v2")
    c.resolveJobDefinition(Some("my-image:v1")) shouldBe Right("my-jd-v1")
    c.resolveJobDefinition(Some("my-image:v2")) shouldBe Right("my-jd-v2")
  }

  test("resolveJobDefinition(Some(x)) fails fast when image is not mapped") {
    val c = minimal.withJobDefinitionForImage("my-image:v1", "my-jd-v1")
    val result = c.resolveJobDefinition(Some("unknown:tag"))
    result.isLeft shouldBe true
    val err = result.left.getOrElse(fail("expected Left"))
    err should include("unknown:tag")
    err should include("withJobDefinitionForImage")
  }

  test(
    "resolveJobDefinition(None) still returns default even when map is set"
  ) {
    minimal
      .withJobDefinitionForImage("my-image:v1", "my-jd-v1")
      .resolveJobDefinition(None) shouldBe Right("default-jd")
  }

  test(
    "resolveJobDefinition(Some(x)) fails when jobDefinitionsByImage is empty"
  ) {
    minimal.resolveJobDefinition(Some("my-image:v1")).isLeft shouldBe true
  }

}
