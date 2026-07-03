package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import tasks.elastic.Deployment
import tasks.util.SimpleSocketAddress
import tasks.util.Uri

class DeploymentScriptTest extends FunSuite with Matchers {

  implicit val config: tasks.util.config.TasksConfig =
    tasks.util.config.parse(() => org.ekrich.config.ConfigFactory.load())

  private def scriptFor(image: Option[String]): String =
    Deployment.script(
      memory = 1024,
      cpu = 2,
      scratch = 4096,
      gpus = Nil,
      masterAddress = SimpleSocketAddress("master.example", 1234),
      masterPrefix = "prefix",
      download = Uri(
        scheme = "http",
        hostname = "code.example",
        port = 8080,
        path = "/"
      ),
      followerHostname = None,
      followerExternalHostname = None,
      followerMayUseArbitraryPort = true,
      followerNodeName = None,
      background = false,
      image = image
    )

  test("Deployment.script includes -Dhosts.image=<x> when image is Some") {
    val script = scriptFor(Some("registry.example/my-image:tag"))
    script should include("-Dhosts.image=registry.example/my-image:tag")
  }

  test("Deployment.script omits -Dhosts.image when image is None") {
    val script = scriptFor(None)
    script should not include ("-Dhosts.image=")
  }

}
