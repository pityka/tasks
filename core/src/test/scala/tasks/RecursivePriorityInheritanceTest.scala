package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import cats.effect.unsafe.implicits.global

import org.scalatest.matchers.should.Matchers

import tasks.jsonitersupport._
import org.ekrich.config.ConfigFactory
import cats.effect.IO

object RecursivePriorityInheritanceTest extends TestHelpers {

  val recordedPriorities =
    new java.util.concurrent.ConcurrentHashMap[Int, Int]()

  val chainTask: ParentTaskDefinition[Input, Int] =
    ParentTask[Input, Int]("recursive-priority-chain", 1) { case input =>
      implicit ce =>
        recordedPriorities.put(input.i, ce.components.priority.s)
        if (input.i <= 0) IO.pure(input.i)
        else
          chainTask(Input(input.i - 1))
    }

  def testConfig2 = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=10
      tasks.cache.enabled = false
      """
    )
  }

  def run(depth: Int) = {
    recordedPriorities.clear()
    withTaskSystem(testConfig2) { implicit ts =>
      chainTask(Input(depth)).map { _ =>
        import scala.jdk.CollectionConverters._
        recordedPriorities.asScala.toMap.map { case (k, v) =>
          (k.toInt, v.toInt)
        }
      }
    }
  }
}

class RecursivePriorityInheritanceTestSuite extends FunSuite with Matchers {

  test("child task inherits parent priority + 1") {
    val priorities =
      RecursivePriorityInheritanceTest.run(4).unsafeRunSync().toOption.get
    // System default priority is 0. The user-submitted root is
    // priorityBase(0) + components.priority(0) + 1 = 1.
    // Each recursive descent adds +1.
    priorities(4) shouldBe 1
    priorities(3) shouldBe 2
    priorities(2) shouldBe 3
    priorities(1) shouldBe 4
    priorities(0) shouldBe 5
  }

}
