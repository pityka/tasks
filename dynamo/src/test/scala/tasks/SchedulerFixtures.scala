package tasks

import tasks.queue.Base64Data
import tasks.queue.HashedTaskDescription
import tasks.queue.QueueImpl
import tasks.queue.Spore
import tasks.queue.TaskId
import tasks.queue.TaskLineage
import tasks.queue.UntypedResult
import tasks.shared._
import tasks.util.message.Address
import tasks.util.message.LauncherName
import tasks.util.message.MessageData.ScheduleTask
import tasks.util.message.MessageData.InputData

private[tasks] object SchedulerFixtures {

  private val emptySpore =
    Spore[AnyRef, AnyRef](fqcn = "", dependencies = Seq.empty)

  def scheduleTask(taskId: String, dataHash: String): ScheduleTask =
    scheduleTask(taskId, dataHash, inlineInput = "")

  def scheduleTask(
      taskId: String,
      dataHash: String,
      inlineInput: String
  ): ScheduleTask =
    ScheduleTask(
      description = HashedTaskDescription(TaskId(taskId, 1), dataHash),
      inputDeserializer = emptySpore,
      outputSerializer = emptySpore,
      function = emptySpore,
      resource = VersionedResourceRequest(
        CodeVersion("v1"),
        ResourceRequest(
          cpu = (1, 1),
          memory = 100,
          scratch = 0,
          gpu = 0,
          image = None,
          nodeSelector = None
        )
      ),
      input = InputData(Base64Data(inlineInput), noCache = false),
      fileServicePrefix = tasks.fileservice.FileServicePrefix(Vector.empty),
      tryCache = true,
      priority = Priority(0),
      labels = Labels.empty,
      lineage = TaskLineage.root,
      proxy = Address("test-proxy")
    )

  def allocated(cpu: Int = 1, memory: Int = 100): VersionedResourceAllocated =
    VersionedResourceAllocated(
      CodeVersion("v1"),
      ResourceAllocated(
        cpu = cpu,
        memory = memory,
        scratch = 0,
        gpu = Nil,
        image = None
      )
    )

  def untypedResult(): UntypedResult =
    UntypedResult(Set.empty, Base64Data(""), None)

  def taskDoneEvent(
      sch: ScheduleTask,
      launcher: LauncherName,
      allocated: VersionedResourceAllocated
  ): QueueImpl.TaskDone = QueueImpl.TaskDone(
    sch = sch,
    launcher = launcher,
    result = tasks.queue.UntypedResultWithMetadata(
      untypedResult = untypedResult(),
      metadata = tasks.queue.ResultMetadata(
        dependencies = Nil,
        started = java.time.Instant.EPOCH,
        ended = java.time.Instant.EPOCH,
        logs = Nil,
        lineage = TaskLineage.root
      ),
      noCache = false
    ),
    elapsedTime = ElapsedTimeNanoSeconds(0L),
    resourceAllocated = allocated.cpuMemoryAllocated
  )

  def resourceAvailable(cpu: Int = 1, memory: Int = 100): ResourceAvailable =
    ResourceAvailable(
      cpu = cpu,
      memory = memory,
      scratch = 0,
      gpu = Nil,
      image = None,
      labels = Set.empty
    )

}
