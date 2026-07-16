package tasks.elastic.batch

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.ekrich.config.ConfigFactory

class BatchJobDefinitionResolutionTest extends AnyFunSuite with Matchers {

  private def cfg(extra: String): BatchConfig = {
    val hocon =
      s"""
         |tasks.elastic.batch {
         |  region = ""
         |  jobQueue = "default-queue"
         |  queues = []
         |  jobDefinition = "default-jd"
         |  minimumCpu = 1
         |  minimumMemory = 512
         |  logGroup = "/aws/batch/job"
         |  tags = []
         |  $extra
         |}
         |""".stripMargin
    new BatchConfig(ConfigFactory.parseString(hocon))
  }

  test("resolveJobDefinition(None) returns the default jobDefinition") {
    val c = cfg("")
    c.resolveJobDefinition(None) shouldBe Right("default-jd")
  }

  test("resolveJobDefinition(Some(x)) returns the mapped job definition") {
    val c = cfg(
      """
        |jobDefinitionsByImage = [
        |  { image = "my-image:v1", jobDefinition = "my-jd-v1" },
        |  { image = "my-image:v2", jobDefinition = "my-jd-v2" }
        |]
        |""".stripMargin
    )
    c.resolveJobDefinition(Some("my-image:v1")) shouldBe Right("my-jd-v1")
    c.resolveJobDefinition(Some("my-image:v2")) shouldBe Right("my-jd-v2")
  }

  test("resolveJobDefinition(Some(x)) fails fast when image is not mapped") {
    val c = cfg(
      """
        |jobDefinitionsByImage = [
        |  { image = "my-image:v1", jobDefinition = "my-jd-v1" }
        |]
        |""".stripMargin
    )
    val result = c.resolveJobDefinition(Some("unknown:tag"))
    result.isLeft shouldBe true
    val err = result.left.getOrElse(fail("expected Left"))
    err should include("unknown:tag")
    err should include("tasks.elastic.batch.jobDefinitionsByImage")
  }

  test("resolveJobDefinition(None) still returns default even when map is set") {
    val c = cfg(
      """
        |jobDefinitionsByImage = [
        |  { image = "my-image:v1", jobDefinition = "my-jd-v1" }
        |]
        |""".stripMargin
    )
    c.resolveJobDefinition(None) shouldBe Right("default-jd")
  }

  test(
    "resolveJobDefinition(Some(x)) fails when jobDefinitionsByImage is absent"
  ) {
    val c = cfg("")
    val result = c.resolveJobDefinition(Some("my-image:v1"))
    result.isLeft shouldBe true
  }

}
