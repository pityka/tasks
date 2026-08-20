package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global

import org.ekrich.config.ConfigFactory
import tasks.jsonitersupport._
import tasks.JvmElasticSupport.JvmGrid

object PackageServerPortInUseTest extends TestHelpers {

  val testTask = tasks.Task[Input, Int]("packageserverportinuse", 1) {
    in => _ => IO(in.i)
  }

  private def adjacentPairWithSecondOccupied: (Int, java.net.ServerSocket) = {
    def attempt(remaining: Int): (Int, java.net.ServerSocket) = {
      val first = new java.net.ServerSocket(0)
      val firstPort = first.getLocalPort
      val occupied =
        try Some(new java.net.ServerSocket(firstPort + 1))
        catch { case _: java.io.IOException => None }
      first.close()
      occupied match {
        case Some(socket) => (firstPort, socket)
        case None if remaining > 0 =>
          attempt(remaining - 1)
        case None =>
          throw new RuntimeException("no adjacent free port pair found")
      }
    }
    attempt(50)
  }

  private val occupiedPackageServerPort =
    Resource.make(IO(adjacentPairWithSecondOccupied)) { case (_, socket) =>
      IO(socket.close())
    }

  def config(messengerPort: Int) = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU = 1
      hosts.port = $messengerPort
      hosts.mayUseArbitraryPort = false
      tasks.cache.enabled = false
      tasks.disableRemoting = false
      tasks.addShutdownHook = false
      tasks.elastic.maxNodes = 0
      """
    )
  }

  def run: IO[Either[cats.effect.ExitCode, Int]] =
    occupiedPackageServerPort.use { case (messengerPort, _) =>
      JvmGrid.make(None, "").use { case (_, elasticSupport) =>
        withTaskSystem(
          config = Some(config(messengerPort)),
          s3Client = Resource.pure(None),
          elasticSupport = Resource.pure(Some(elasticSupport)),
          externalQueueState = Resource.pure(None)
        ) { implicit ts =>
          testTask(Input(42))(tasks.ResourceRequest(1, 500))
        }
      }
    }

}

class PackageServerPortInUseTestSuite extends FunSuite with Matchers {

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Warn))
    .replace()

  test(
    "a task system starts and runs tasks when the port the package server prefers is already taken"
  ) {
    PackageServerPortInUseTest.run.unsafeRunSync() shouldBe Right(42)
  }

}
