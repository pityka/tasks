package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import cats.effect.unsafe.implicits.global

import org.scalatest.matchers.should.Matchers

import tasks.jsonitersupport._
import org.ekrich.config.ConfigFactory
import cats.effect.IO
import scala.concurrent.duration._

object ReleaseResourcesParentChildTest extends TestHelpers {

  val childTask: TaskDefinition[Input, Int] =
    Task[Input, Int]("rre-parent-child-child", 1) { case input => implicit ce =>
      IO.pure(input.i * 2)
    }

  val parentTask: TaskDefinition[Input, Int] =
    Task[Input, Int]("rre-parent-child-parent", 1) { case input => implicit ce =>
      for {
        _ <- releaseResourcesEarly
        r <- childTask(Input(input.i))(ResourceRequest(1, 1))
      } yield r
    }

  def testConfig2 = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=1
      tasks.cache.enabled = false
      """
    )
  }

  def run(n: Int) =
    withTaskSystem(testConfig2) { implicit ts =>
      IO.parSequenceN(n)((1 to n).toList.map { i =>
        parentTask(Input(i))(ResourceRequest(1, 1)).map(_ => i)
      }).timeout(15.seconds)
    }
}

class ReleaseResourcesParentChildTestSuite extends FunSuite with Matchers {

  test(
    "many parent-child pairs run to completion on a 1-CPU pool when parents release resources before submitting child"
  ) {
    val n = 50
    val results =
      ReleaseResourcesParentChildTest.run(n).unsafeRunSync().toOption.get
    results.sorted shouldBe (1 to n).toList
  }

}
