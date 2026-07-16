package tasks.elastic.ec2

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.ekrich.config.ConfigFactory
import tasks.util.{SimpleSocketAddress, Uri}
import tasks.util.config.parse

class EC2UserDataTest extends AnyFunSuite with Matchers {

  private implicit val tasksConfig: tasks.util.config.TasksConfig =
    parse(() => ConfigFactory.load())

  private def script(mount: String): String =
    EC2UserData.script(
      memory = 2000,
      cpu = 4,
      scratch = 100000,
      gpus = Nil,
      masterAddress = SimpleSocketAddress("master.example", 1234),
      masterPrefix = "prefix",
      codeDownload = Uri("http", "code.example", 8080, "/"),
      image = None,
      labels = Set.empty,
      mountPoint = mount
    )

  test("script contains the instance-store mount block") {
    val s = script("/instancestorage")
    s should include("nvme-Amazon_EC2_NVMe_Instance_Storage_")
    s should include("mkfs.xfs")
    s should include("mdadm --create /dev/md0")
    s should include("chmod 1777")
  }

  test("script wires java.io.tmpdir to the mount point via _JAVA_OPTIONS") {
    val s = script("/instancestorage")
    s should include("-Djava.io.tmpdir=$MOUNT")
    s should include("_JAVA_OPTIONS")
  }

  test("script terminates its own instance when the JVM exits") {
    val s = script("/instancestorage")
    s should include("http://169.254.169.254/latest/api/token")
    s should include("X-aws-ec2-metadata-token")
    s should include("aws --region \"$REGION\" ec2 terminate-instances")
  }

  test("script starts with a shebang and enables strict mode") {
    val s = script("/instancestorage")
    s should startWith("#!/usr/bin/env bash")
    s should include("set -u")
  }

  test("mount point is shell-quoted so paths with spaces survive") {
    val s = script("/mnt/instance store")
    s should include("MOUNT='/mnt/instance store'")
  }

  test("shellQuote escapes embedded single quotes") {
    EC2UserData.shellQuote("a'b") shouldBe "'a'\"'\"'b'"
  }

  test("labels propagate into the JVM launch") {
    val s = EC2UserData.script(
      memory = 1000,
      cpu = 1,
      scratch = 0,
      gpus = Nil,
      masterAddress = SimpleSocketAddress("m", 1),
      masterPrefix = "p",
      codeDownload = Uri("http", "h", 1, "/"),
      image = None,
      labels = Set("gpu:a100"),
      mountPoint = "/tmp"
    )
    s should include("-Dhosts.labelsAsCommaString=gpu:a100")
  }

  test("image propagates into the JVM launch") {
    val s = EC2UserData.script(
      memory = 1000,
      cpu = 1,
      scratch = 0,
      gpus = Nil,
      masterAddress = SimpleSocketAddress("m", 1),
      masterPrefix = "p",
      codeDownload = Uri("http", "h", 1, "/"),
      image = Some("registry.example/img:tag"),
      labels = Set.empty,
      mountPoint = "/tmp"
    )
    s should include("-Dhosts.image=registry.example/img:tag")
  }
}
