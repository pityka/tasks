/*
 * The MIT License
 *
 * Copyright (c) 2026 Istvan Bartha
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 */

package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import tasks.queue._
import tasks.shared._
import tasks.util.message._
import tasks.util.message.MessageData.{InputData, ScheduleTask}
import tasks.fileservice.FileServicePrefix

/** Pure unit tests for QueueImpl.selectPreemptionVictims. These cover
  * branches that the system-level PreemptionStallResolveTest doesn't
  * exercise (multi-victim selection, no-valid-victim).
  */
class PreemptionDecisionTest extends FunSuite with Matchers {

  private val codeVersion = CodeVersion("test")
  private val unitSpore: Spore[AnyRef, AnyRef] =
    Spore("unused-for-decision-tests", Nil)

  private def taskId(name: String): TaskId = TaskId(name, 1)

  private def hash(name: String, dataHash: String): HashedTaskDescription =
    HashedTaskDescription(taskId(name), dataHash)

  private def invocationId(name: String, dataHash: String): TaskInvocationId = {
    val h = hash(name, dataHash)
    TaskInvocationId(h.taskId, h)
  }

  private def request(cpu: Int, memory: Int = 1): VersionedResourceRequest =
    VersionedResourceRequest(codeVersion, cpu, memory, 0, 0, None)

  private def allocated(cpu: Int, memory: Int = 1): VersionedResourceAllocated =
    VersionedResourceAllocated(
      codeVersion,
      ResourceAllocated(cpu, memory, 0, Nil, None)
    )

  private def available(cpu: Int, memory: Int = 1): VersionedResourceAvailable =
    VersionedResourceAvailable(
      codeVersion,
      ResourceAvailable(cpu, memory, 0, Nil, None)
    )

  private def schTask(
      name: String,
      dataHash: String,
      lineage: Seq[TaskInvocationId],
      resource: VersionedResourceRequest = request(1)
  ): ScheduleTask =
    ScheduleTask(
      description = hash(name, dataHash),
      inputDeserializer = unitSpore,
      outputSerializer = unitSpore,
      function = unitSpore,
      resource = resource,
      input = InputData(Base64Data(""), noCache = false),
      fileServicePrefix = FileServicePrefix(Vector.empty),
      tryCache = true,
      priority = Priority(0),
      labels = Labels.empty,
      lineage = TaskLineage(lineage),
      proxy = Address("proxy-" + dataHash),
      filePrefix = ""
    )

  private val launcherA = LauncherName("launcher-A")
  private val launcherB = LauncherName("launcher-B")

  /** Stall constructed with two ancestors of q both on launcher A.
    * launcher A's free resources can't fit q on its own, and neither
    * ancestor can alone be added back to fit q. Together they free
    * enough — multi-victim must select both.
    */
  test("multi-victim: two parents on one launcher together free room for q") {
    val parent1 = invocationId("parent1", "h1")
    val parent2 = invocationId("parent2", "h2")
    val q = schTask(
      "child",
      "hq",
      lineage = Seq(parent1, parent2),
      resource = request(4)
    )
    val schParent1 = schTask("parent1", "h1", lineage = Seq.empty)
    val schParent2 = schTask("parent2", "h2", lineage = Seq(parent1))
    val keyP1 = QueueImpl.project(schParent1)
    val keyP2 = QueueImpl.project(schParent2)

    val state = QueueImpl.State.empty.copy(
      queuedTasks = Map(QueueImpl.project(q) -> ((q, Nil))),
      scheduledTasks = Map(
        keyP1 -> ((launcherA, allocated(2), Nil, schParent1)),
        keyP2 -> ((launcherA, allocated(2), Nil, schParent2))
      ),
      availableResourcesByLauncher = Map(launcherA -> available(0))
    )

    val decision = QueueImpl.selectPreemptionVictims(state)
    decision match {
      case QueueImpl.PreemptionDecision.Cancel(launcher, victims) =>
        launcher should equal(launcherA)
        victims.map(_._1).toSet should equal(Set(keyP1, keyP2))
      case other =>
        fail(s"Expected Cancel with two victims, got $other")
    }
  }

  /** Stall holds but no candidate can free enough resources. Should
    * report Unresolvable rather than picking something arbitrary.
    */
  test("unresolvable: ancestors exist but none free enough room for q") {
    val parent = invocationId("parent", "h1")
    val q = schTask(
      "child",
      "hq",
      lineage = Seq(parent),
      resource = request(8)
    )
    val schParent = schTask("parent", "h1", lineage = Seq.empty)
    val keyP = QueueImpl.project(schParent)

    val state = QueueImpl.State.empty.copy(
      queuedTasks = Map(QueueImpl.project(q) -> ((q, Nil))),
      scheduledTasks = Map(
        keyP -> ((launcherA, allocated(1), Nil, schParent))
      ),
      availableResourcesByLauncher = Map(launcherA -> available(0))
    )

    QueueImpl.selectPreemptionVictims(state) should equal(
      QueueImpl.PreemptionDecision.Unresolvable
    )
  }

  /** Two independent stuck chains on different launchers. The
    * decision picks one of them; the other will be resolved on a
    * subsequent tick. Verifies victim selection respects per-launcher
    * availability.
    */
  test("multi-launcher: picks a victim on the launcher whose chain matches") {
    val parentA = invocationId("parentA", "hA1")
    val parentB = invocationId("parentB", "hB1")
    val qA = schTask("childA", "hAq", lineage = Seq(parentA))
    val qB = schTask("childB", "hBq", lineage = Seq(parentB))
    val schParentA = schTask("parentA", "hA1", lineage = Seq.empty)
    val schParentB = schTask("parentB", "hB1", lineage = Seq.empty)
    val keyPA = QueueImpl.project(schParentA)
    val keyPB = QueueImpl.project(schParentB)

    val state = QueueImpl.State.empty.copy(
      queuedTasks = Map(
        QueueImpl.project(qA) -> ((qA, Nil)),
        QueueImpl.project(qB) -> ((qB, Nil))
      ),
      scheduledTasks = Map(
        keyPA -> ((launcherA, allocated(1), Nil, schParentA)),
        keyPB -> ((launcherB, allocated(1), Nil, schParentB))
      ),
      availableResourcesByLauncher = Map(
        launcherA -> available(0),
        launcherB -> available(0)
      )
    )

    val decision = QueueImpl.selectPreemptionVictims(state)
    decision match {
      case QueueImpl.PreemptionDecision.Cancel(launcher, victims) =>
        // One launcher's chain is picked, with that launcher's parent
        // as the single victim. Either A or B is valid.
        Set(launcherA, launcherB) should contain(launcher)
        victims.size should equal(1)
        if (launcher == launcherA) victims.head._1 should equal(keyPA)
        else victims.head._1 should equal(keyPB)
      case other =>
        fail(s"Expected Cancel, got $other")
    }
  }

  /** Tasks already in cancelInFlight are excluded from victim
    * selection — otherwise we'd cancel them twice on subsequent ticks.
    */
  test("cancelInFlight tasks are filtered out of victim candidates") {
    val parent = invocationId("parent", "h1")
    val q = schTask("child", "hq", lineage = Seq(parent))
    val schParent = schTask("parent", "h1", lineage = Seq.empty)
    val keyP = QueueImpl.project(schParent)

    val state = QueueImpl.State.empty.copy(
      queuedTasks = Map(QueueImpl.project(q) -> ((q, Nil))),
      scheduledTasks = Map(
        keyP -> ((launcherA, allocated(1), Nil, schParent))
      ),
      availableResourcesByLauncher = Map(launcherA -> available(0)),
      cancelInFlight = Set(keyP)
    )

    QueueImpl.selectPreemptionVictims(state) should equal(
      QueueImpl.PreemptionDecision.Unresolvable
    )
  }

  /** Guard: if any launcher already has free capacity for any queued
    * task, preemption is not needed (the launcher will pick it up on
    * its next askForWork).
    */
  test(
    "not stalled when a launcher already has room for some queued task"
  ) {
    val parent = invocationId("parent", "h1")
    val q = schTask("child", "hq", lineage = Seq(parent))
    val schParent = schTask("parent", "h1", lineage = Seq.empty)
    val keyP = QueueImpl.project(schParent)

    val state = QueueImpl.State.empty.copy(
      queuedTasks = Map(QueueImpl.project(q) -> ((q, Nil))),
      scheduledTasks = Map(
        keyP -> ((launcherA, allocated(1), Nil, schParent))
      ),
      availableResourcesByLauncher = Map(launcherA -> available(4))
    )

    QueueImpl.selectPreemptionVictims(state) should equal(
      QueueImpl.PreemptionDecision.NotStalled
    )
  }

  /** A scheduled task with no descendants in Q ∪ S violates the
    * "every scheduled task has a descendant" condition — system is
    * making progress, no preemption.
    */
  test("not stalled when a scheduled task has no descendants") {
    val schParent = schTask("parent", "h1", lineage = Seq.empty)
    val schLeaf = schTask("leaf", "h2", lineage = Seq.empty)
    val q = schTask("standalone", "h3", lineage = Seq.empty)

    val state = QueueImpl.State.empty.copy(
      queuedTasks = Map(QueueImpl.project(q) -> ((q, Nil))),
      scheduledTasks = Map(
        QueueImpl.project(schParent) -> (
          (launcherA, allocated(1), Nil, schParent)
        ),
        QueueImpl.project(schLeaf) -> (
          (launcherA, allocated(1), Nil, schLeaf)
        )
      ),
      availableResourcesByLauncher = Map(launcherA -> available(0))
    )

    QueueImpl.selectPreemptionVictims(state) should equal(
      QueueImpl.PreemptionDecision.NotStalled
    )
  }
}
