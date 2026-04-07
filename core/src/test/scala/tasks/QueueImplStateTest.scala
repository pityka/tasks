package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import tasks.queue.QueueImpl
import tasks.util.message.LauncherName

class QueueImplStateTestSuite extends FunSuite with Matchers {

  test("first Incremented event should set counter to 1, not 0") {
    val launcher = LauncherName("test-launcher")
    val state = QueueImpl.State.empty

    // Before any increment, counter doesn't exist
    state.counters.get(launcher) shouldBe None

    // First increment should produce 1
    val afterFirst = state.update(QueueImpl.Incremented(launcher))
    afterFirst.counters(launcher) should equal(1L)

    // Second increment should produce 2
    val afterSecond = afterFirst.update(QueueImpl.Incremented(launcher))
    afterSecond.counters(launcher) should equal(2L)
  }

  test(
    "heartbeat query should see different values before and after first increment"
  ) {
    val launcher = LauncherName("test-launcher")
    val state = QueueImpl.State.empty

    // Heartbeat query uses getOrElse(0L) for missing counters
    val beforeIncrement = state.counters.get(launcher).getOrElse(0L)
    beforeIncrement should equal(0L)

    val afterIncrement =
      state.update(QueueImpl.Incremented(launcher))
    val afterIncrementValue =
      afterIncrement.counters.get(launcher).getOrElse(0L)

    // Must differ from the pre-increment value to avoid false timeout detection
    afterIncrementValue should not equal beforeIncrement
  }

}
