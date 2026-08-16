package tasks

import scala.concurrent.duration._

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers
import org.ekrich.config.ConfigFactory

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global

import tasks.jsonitersupport._
import tasks.JvmElasticSupport.JvmGrid
import tasks.elastic.CompositeElasticSupport
import tasks.elastic.CompositeMember

object CompositeElasticGridTest extends TestHelpers {

  val whichLauncher =
    Task[Input, String]("whichLauncher-composite", 1) { _ => implicit ce =>
      IO.pure(launcherName.name)
    }

  def imageRequest(image: String)(implicit
      cv: tasks.shared.CodeVersion
  ): tasks.shared.VersionedResourceRequest =
    tasks.shared.VersionedResourceRequest(
      cv,
      tasks.shared.ResourceRequest(
        cpu = (1, 1),
        memory = 100,
        scratch = 0,
        gpu = 0,
        image = Some(image),
        nodeSelector = None
      )
    )

  def baseConfig(maxNodes: Int) = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
         |hosts.numCPU = 0
         |tasks.disableRemoting = false
         |tasks.addShutdownHook = false
         |tasks.elastic.maxNodes = $maxNodes
         |tasks.elastic.maxNodesCumulative = ${maxNodes * 4}
         |tasks.failuredetector.acceptable-heartbeat-pause = 10 s
         |""".stripMargin
    )
  }
}

class CompositeElasticGridTestSuite extends FunSuite with Matchers {
  import CompositeElasticGridTest._

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Info))
    .replace()

  test("a composite spawns each node on the member which accepts its request") {
    val program = JvmGrid
      .makeWithPrefix(
        nodeNamePrefix = "special-",
        externalQueueState = None,
        extraWorkerConfig = "",
        labelsForRequest = _ => Set.empty
      )
      .use { case (specialControl, specialSupport) =>
        JvmGrid
          .makeWithPrefix(
            nodeNamePrefix = "default-",
            externalQueueState = None,
            extraWorkerConfig = "",
            labelsForRequest = _ => Set.empty
          )
          .use { case (defaultControl, defaultSupport) =>
            val composite = CompositeElasticSupport(
              List(
                CompositeMember(
                  "special",
                  specialSupport,
                  _.startsWith("special-")
                ).accepting(_.image.contains("img-special")),
                CompositeMember.catchAll("default", defaultSupport)
              )
            )

            withTaskSystem(
              baseConfig(maxNodes = 4),
              Resource.pure(None),
              Resource.pure(Some(composite))
            ) { implicit ts =>
              for {
                onSpecial <- whichLauncher(Input(1))(
                  imageRequest("img-special")
                )
                onDefault <- whichLauncher(Input(2))(
                  imageRequest("img-default")
                )
                specialNodes <- specialControl.list
                defaultNodes <- defaultControl.list
              } yield (onSpecial, onDefault, specialNodes, defaultNodes)
            }.timeout(120.seconds)
          }
      }

    val (onSpecial, onDefault, specialNodes, defaultNodes) =
      program.unsafeRunSync().toOption.get

    onSpecial should not be onDefault

    specialNodes.size shouldBe 1
    all(specialNodes) should startWith("special-")

    defaultNodes.size shouldBe 1
    all(defaultNodes) should startWith("default-")
  }
}
