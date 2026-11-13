package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem

import tasks.queue.DynamoDiff
import tasks.queue.DynamoQueueState
import tasks.queue.Proxy
import tasks.queue.QueueImpl
import tasks.queue.QueueImpl.State
import tasks.util.message.Address
import tasks.util.message.LauncherName
import tasks.util.message.MessageData.ScheduleTask

class DynamoDiffPlanTest extends FunSuite with Matchers {

  private val table = "plan-table"

  private def kinds(ops: List[TransactWriteItem]): List[String] =
    ops.map { op =>
      if (op.put() != null) "put"
      else if (op.delete() != null) "delete"
      else if (op.conditionCheck() != null) "check"
      else if (op.update() != null) "update"
      else "unknown"
    }

  private def versions(base: Map[ScheduleTask, Long]): Map[String, Long] =
    base.map { case (sch, v) => sch.description.hash -> v }

  test("no-op diff produces an empty plan") {
    val plan = DynamoDiff.plan(
      table = table,
      oldState = State.empty,
      newState = State.empty,
      baseTaskVersions = Map.empty,
      baseResultChunkVersions = Map.empty,
      baseResourcesVersion = 0L
    )
    plan.chunks shouldBe empty
  }

  test("resources-only diff produces a single-chunk plan with just one put") {
    val plan = DynamoDiff.plan(
      table = table,
      oldState = State.empty,
      newState = State.empty.update(
        QueueImpl.LauncherJoined(LauncherName("l"), None)
      ),
      baseTaskVersions = Map.empty,
      baseResultChunkVersions = Map.empty,
      baseResourcesVersion = 0L
    )
    plan.chunks.size shouldBe 1
    kinds(plan.chunks.head) shouldBe List("put")
  }

  test(
    "a single new task plus a resources change becomes one chunk with one put and one resources put"
  ) {
    val sch = SchedulerFixtures.scheduleTask("one", "one-hash")
    val proxy = Proxy(Address("p-one"))
    val newState = State.empty
      .update(QueueImpl.MainProcessJoined("session-a"))
      .update(QueueImpl.Enqueued(sch, List(proxy), 0L))
    val plan = DynamoDiff.plan(
      table = table,
      oldState = State.empty,
      newState = newState,
      baseTaskVersions = Map.empty,
      baseResultChunkVersions = Map.empty,
      baseResourcesVersion = 0L
    )
    plan.chunks.size shouldBe 1
    kinds(plan.chunks.head).sorted shouldBe List("put", "put").sorted
  }

  test(
    "a task-only diff embeds a resources ConditionCheck barrier in the chunk"
  ) {
    val sch = SchedulerFixtures.scheduleTask("only-task", "ot-hash")
    val proxy = Proxy(Address("p-ot"))
    val plan = DynamoDiff.plan(
      table = table,
      oldState = State.empty,
      newState = State.empty.update(QueueImpl.Enqueued(sch, List(proxy), 0L)),
      baseTaskVersions = Map.empty,
      baseResultChunkVersions = Map.empty,
      baseResourcesVersion = 5L
    )
    plan.chunks.size shouldBe 1
    kinds(plan.chunks.head) shouldBe List("put", "check")
    val check = plan.chunks.head.last.conditionCheck()
    check.key().get("pk").s() shouldBe DynamoQueueState.ResourcesPartitionKey
    val expectedVer = check.expressionAttributeValues().get(":ver").n()
    expectedVer shouldBe "5"
  }

  test(
    "a diff with 150 new tasks and a resources change chunks across multiple TransactWriteItems"
  ) {
    val bulk = (0 until 150).toList.map(i =>
      SchedulerFixtures.scheduleTask(s"bulk-$i", s"bulk-h-$i")
    )
    val proxy = Proxy(Address("p-bulk"))
    val newState = bulk
      .foldLeft(State.empty) { (acc, sch) =>
        acc.update(QueueImpl.Enqueued(sch, List(proxy), 0L))
      }
      .update(QueueImpl.MainProcessJoined("s"))
    val plan = DynamoDiff.plan(
      table = table,
      oldState = State.empty,
      newState = newState,
      baseTaskVersions = Map.empty,
      baseResultChunkVersions = Map.empty,
      baseResourcesVersion = 0L
    )
    plan.chunks.size should be > 1
    val resourcesPutInLast =
      plan.chunks.last.exists { op =>
        op.put() != null &&
        op.put().item().get("pk").s() == DynamoQueueState.ResourcesPartitionKey
      }
    resourcesPutInLast shouldBe true
    val resourcesPutInEarlier =
      plan.chunks.init.exists { chunk =>
        chunk.exists { op =>
          op.put() != null &&
          op.put()
            .item()
            .get("pk")
            .s() == DynamoQueueState.ResourcesPartitionKey
        }
      }
    resourcesPutInEarlier shouldBe false
    plan.chunks.init.foreach { chunk =>
      kinds(chunk).contains("check") shouldBe true
    }
  }

  test(
    "diff that deletes a task emits a Delete op with the task's base version in the condition"
  ) {
    val sch = SchedulerFixtures.scheduleTask("del", "del-hash")
    val proxy = Proxy(Address("p-del"))
    val oldState = State.empty.update(QueueImpl.Enqueued(sch, List(proxy), 0L))
    val baseVer = 7L
    val plan = DynamoDiff.plan(
      table = table,
      oldState = oldState,
      newState = State.empty,
      baseTaskVersions = Map(sch.description.hash -> baseVer),
      baseResultChunkVersions = Map.empty,
      baseResourcesVersion = 0L
    )
    plan.chunks.size shouldBe 1
    val deleteOp = plan.chunks.head.find(_.delete() != null).get.delete()
    deleteOp.key().get("sk").s() shouldBe sch.description.hash
    deleteOp
      .expressionAttributeValues()
      .get(":ver")
      .n() shouldBe baseVer.toString
  }

  test(
    "diff that updates a task emits a Put op with the task's base version in the condition"
  ) {
    val sch = SchedulerFixtures.scheduleTask("upd", "upd-hash")
    val proxyA = Proxy(Address("p-upd-a"))
    val proxyB = Proxy(Address("p-upd-b"))
    val oldState = State.empty.update(QueueImpl.Enqueued(sch, List(proxyA), 0L))
    val newState = State.empty
      .update(QueueImpl.Enqueued(sch, List(proxyA), 0L))
      .update(QueueImpl.Enqueued(sch, List(proxyB), 0L))
    val baseVer = 3L
    val plan = DynamoDiff.plan(
      table = table,
      oldState = oldState,
      newState = newState,
      baseTaskVersions = Map(sch.description.hash -> baseVer),
      baseResultChunkVersions = Map.empty,
      baseResourcesVersion = 0L
    )
    val putOp = plan.chunks.head
      .find(op =>
        op.put() != null &&
          op.put().item().get("pk").s() == DynamoQueueState.TasksPartitionKey
      )
      .get
      .put()
    putOp.conditionExpression() shouldBe "#ver = :ver"
    putOp.expressionAttributeValues().get(":ver").n() shouldBe baseVer.toString
    putOp.item().get("ver").n() shouldBe (baseVer + 1).toString
  }

  test(
    "diff that inserts a brand-new task uses attribute_not_exists condition and ver=1"
  ) {
    val sch = SchedulerFixtures.scheduleTask("ins", "ins-hash")
    val proxy = Proxy(Address("p-ins"))
    val plan = DynamoDiff.plan(
      table = table,
      oldState = State.empty,
      newState = State.empty.update(QueueImpl.Enqueued(sch, List(proxy), 0L)),
      baseTaskVersions = Map.empty,
      baseResultChunkVersions = Map.empty,
      baseResourcesVersion = 0L
    )
    val putOp = plan.chunks.head
      .find(op =>
        op.put() != null &&
          op.put().item().get("pk").s() == DynamoQueueState.TasksPartitionKey
      )
      .get
      .put()
    putOp.conditionExpression() shouldBe "attribute_not_exists(#pk)"
    putOp.item().get("ver").n() shouldBe "1"
  }

}
