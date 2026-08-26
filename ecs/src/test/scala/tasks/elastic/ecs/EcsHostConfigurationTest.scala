package tasks.elastic.ecs

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.ekrich.config.ConfigFactory

import tasks.util.config.TasksConfig

class EcsHostConfigurationTest extends AnyFunSuite with Matchers {

  private def configWith(labels: String): TasksConfig = {
    val storage = {
      val tmp = java.io.File.createTempFile("tasks-ecs-host", ".temp")
      tmp.delete()
      tmp.getAbsolutePath
    }
    tasks.util.config.parse(() =>
      tasks.util.loadConfig(
        Some(
          ConfigFactory.parseString(
            s"""tasks.fileservice.storageURI=$storage
               |hosts.mayUseArbitraryPort = true
               |hosts.labelsAsCommaString = "$labels"
               |""".stripMargin
          )
        )
      )
    )
  }

  test("discovered attributes are advertised alongside the configured labels") {
    val hostConfig =
      new EcsHostConfiguration(configWith("from-config"), Set("zone:a", "gpu"))

    hostConfig.labels shouldBe Set("from-config", "zone:a", "gpu")
  }

  test("without discovered attributes only the configured labels remain") {
    val hostConfig =
      new EcsHostConfiguration(configWith("from-config"), Set.empty)

    hostConfig.labels shouldBe Set("from-config")
  }

  test("a discovered attribute which is also configured is not duplicated") {
    val hostConfig =
      new EcsHostConfiguration(configWith("zone:a"), Set("zone:a", "gpu"))

    hostConfig.labels shouldBe Set("zone:a", "gpu")
  }
}
