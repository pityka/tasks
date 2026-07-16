package tasks.elastic.ec2

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.ekrich.config.{ConfigException, ConfigFactory}

class EC2ConfigTest extends AnyFunSuite with Matchers {

  private def cfg(extra: String): EC2Config = {
    val overrides = ConfigFactory.parseString(
      s"""tasks.elastic.aws {
         |  $extra
         |}""".stripMargin
    )
    val merged = overrides.withFallback(ConfigFactory.load("reference.conf"))
    new EC2Config(merged.resolve)
  }

  test("fails fast when subnetId is empty") {
    val ex = intercept[ConfigException.BadValue] {
      cfg(
        """subnetId = ""
          |instanceTypes = ["m6i.large"]
          |ami = "ami-x"
          |""".stripMargin
      )
    }
    ex.getMessage should include("subnetId")
  }

  test("fails fast when instanceTypes is empty") {
    val ex = intercept[ConfigException.BadValue] {
      cfg(
        """subnetId = "subnet-abc"
          |instanceTypes = []
          |ami = "ami-x"
          |""".stripMargin
      )
    }
    ex.getMessage should include("instanceTypes")
  }

  test("iamRoleArn rejects non-ARN strings") {
    val ex = intercept[ConfigException.BadValue] {
      cfg(
        """subnetId = "subnet-abc"
          |instanceTypes = ["m6i.large"]
          |iamRoleArn = "my-role"
          |""".stripMargin
      )
    }
    ex.getMessage should include("ARN")
  }

  test("iamRoleArn accepts a valid ARN") {
    val c = cfg(
      """subnetId = "subnet-abc"
        |instanceTypes = ["m6i.large"]
        |iamRoleArn = "arn:aws:iam::123456789012:instance-profile/my-role"
        |""".stripMargin
    )
    c.iamRoleArn shouldBe defined
    c.iamRoleArn.get should startWith("arn:")
  }

  test("iamRoleArn is None when empty string") {
    val c = cfg(
      """subnetId = "subnet-abc"
        |instanceTypes = ["m6i.large"]
        |iamRoleArn = ""
        |""".stripMargin
    )
    c.iamRoleArn shouldBe empty
  }

  test("instanceTags parses list of key/value objects") {
    val c = cfg(
      """subnetId = "subnet-abc"
        |instanceTypes = ["m6i.large"]
        |tags = [
        |  { key = "env",     value = "prod" }
        |  { key = "project", value = "tasks" }
        |]
        |""".stripMargin
    )
    c.instanceTags shouldBe List("env" -> "prod", "project" -> "tasks")
  }

  test("empty tags list parses to empty list") {
    val c = cfg(
      """subnetId = "subnet-abc"
        |instanceTypes = ["m6i.large"]
        |tags = []
        |""".stripMargin
    )
    c.instanceTags shouldBe empty
  }

  test("candidateInstanceTypes filters empty strings and preserves order") {
    val c = cfg(
      """subnetId = "subnet-abc"
        |instanceTypes = ["m6i.large", "", "c6i.xlarge"]
        |""".stripMargin
    )
    c.candidateInstanceTypes shouldBe List("m6i.large", "c6i.xlarge")
  }

  test("securityGroups filters empty and dedupes") {
    val c = cfg(
      """subnetId = "subnet-abc"
        |instanceTypes = ["m6i.large"]
        |securityGroups = ["sg-a", "", "sg-a", "sg-b"]
        |""".stripMargin
    )
    c.securityGroups shouldBe List("sg-a", "sg-b")
  }

  test("instanceStorageMountPoint defaults to /instancestorage") {
    val c = cfg(
      """subnetId = "subnet-abc"
        |instanceTypes = ["m6i.large"]
        |""".stripMargin
    )
    c.instanceStorageMountPoint shouldBe "/instancestorage"
  }
}
