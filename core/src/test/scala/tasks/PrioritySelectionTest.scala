package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import cats.effect.unsafe.implicits.global

import org.scalatest.matchers.should.Matchers

import tasks.jsonitersupport._
import org.ekrich.config.ConfigFactory
import cats.effect.IO

object PrioritySelectionTest extends TestHelpers {

  val executionOrder = new java.util.concurrent.ConcurrentLinkedQueue[Int]()

  val priorityTask = Task[Input, Int]("priorityseltest", 1) { case input =>
    implicit ce =>
      IO {
        executionOrder.add(input.i)
        input.i
      }
  }

  def testConfig2 = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=1
      tasks.cache.enabled = false
      tasks.askInterval = 3 seconds
      """
    )
  }

  def run = {
    executionOrder.clear()
    withTaskSystem(testConfig2) { implicit ts =>
      // First poll fires immediately on empty queue.
      // We have askInterval (3s) to submit all tasks before the next poll.
      val lowPrio = priorityTask(Input(1))(
        ResourceRequest(1, 500),
        priorityBase = tasks.shared.Priority(1)
      )
      val highPrio = priorityTask(Input(2))(
        ResourceRequest(1, 500),
        priorityBase = tasks.shared.Priority(100)
      )
      for {
        _ <- IO.both(lowPrio, highPrio)
      } yield {
        import scala.jdk.CollectionConverters._
        executionOrder.asScala.toList
      }
    }
  }

}

class PrioritySelectionTestSuite extends FunSuite with Matchers {

  test(
    "higher priority task should be selected before lower priority task"
  ) {
    val order =
      PrioritySelectionTest.run.unsafeRunSync().toOption.get
    // Input(2) has priority 100, Input(1) has priority 1
    // The high-priority task (input=2) should execute first
    order.head should equal(2)
  }

}
