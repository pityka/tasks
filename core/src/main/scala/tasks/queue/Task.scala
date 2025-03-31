/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Modified work, Copyright (c) 2016 Istvan Bartha

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
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package tasks.queue

import scala.util._

import tasks.fileservice._
import tasks.util.config._
import tasks.shared._
import tasks._
import tasks.util.message._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

import java.time.Instant
import cats.effect.IO
import tasks.TaskSystemComponents
import tasks.caching.TaskResultCache
import tasks.util.Messenger
import tasks.queue.Launcher.LauncherHandle

case class UntypedResult(
    files: Set[SharedFile],
    data: Base64Data,
    mutableFiles: Option[Set[SharedFile]]
)

private[tasks] case class DependenciesAndRuntimeMetadata(
    dependencies: Seq[History],
    logs: Seq[LogRecord]
)

private[tasks] object DependenciesAndRuntimeMetadata {
  implicit val codec: JsonValueCodec[DependenciesAndRuntimeMetadata] =
    JsonCodecMaker.make

}

private[tasks] case class TaskInvocationId(
    id: TaskId,
    description: HashedTaskDescription
) {
  override def toString = id.id + "_" + description.hash
}
private[tasks] object TaskInvocationId {
  implicit val codec: JsonValueCodec[TaskInvocationId] = JsonCodecMaker.make
}

private[tasks] case class TaskLineage(lineage: Seq[TaskInvocationId]) {
  def leaf = lineage.last
  def inherit(td: HashedTaskDescription) =
    TaskLineage(
      lineage :+ TaskInvocationId(td.taskId, td)
    )
}

private[tasks] object TaskLineage {
  def root = TaskLineage(Seq.empty)
  implicit val codec: JsonValueCodec[TaskLineage] = JsonCodecMaker.make
}

private[tasks] case class ResultMetadata(
    dependencies: Seq[History],
    started: Instant,
    ended: Instant,
    logs: Seq[LogRecord],
    lineage: TaskLineage
)

private[tasks] object ResultMetadata {
  implicit val codec: JsonValueCodec[ResultMetadata] = JsonCodecMaker.make
}

final case class UntypedResultWithMetadata(
    private[tasks] untypedResult: UntypedResult,
    private[tasks] metadata: ResultMetadata,
    private[tasks] noCache: Boolean
)

object UntypedResultWithMetadata {
  implicit val codec: JsonValueCodec[UntypedResultWithMetadata] =
    JsonCodecMaker.make(
      CodecMakerConfig
        .withMapMaxInsertNumber(2147483645)
        .withSetMaxInsertNumber(2147483645)
    )
}

private[tasks] object UntypedResult {

  private def immutableFiles(r: Any): Set[SharedFile] =
    HasSharedFiles.recurse(r)(_.immutableFiles).toSet

  private def mutableFiles(r: Any): Set[SharedFile] =
    HasSharedFiles.recurse(r)(_.mutableFiles).toSet

  def make[A](r: A)(implicit ser: Serializer[A]): UntypedResult = {
    val mut = mutableFiles(r)
    val immut = immutableFiles(r) &~ mut
    val m = if (mut.isEmpty) None else Some(mut)
    UntypedResult(immut, Base64DataHelpers(ser(r)), m)
  }

  implicit val codec: JsonValueCodec[UntypedResult] =
    JsonCodecMaker.make(
      CodecMakerConfig
        .withMapMaxInsertNumber(2147483645)
        .withSetMaxInsertNumber(2147483645)
    )
}

class ComputationEnvironment(
    val resourceAllocated: ResourceAllocated,
    implicit val components: TaskSystemComponents,
    implicit val launcher: LauncherHandle,
    val taskActor: Task,
    val taskHash: HashedTaskDescription
) {

  private val logQueue =
    new java.util.concurrent.ConcurrentLinkedQueue[LogRecord]()

  def appendLog(l: LogRecord) = logQueue.add(l)

  def currentLogRecords = {
    import scala.jdk.CollectionConverters._
    logQueue.iterator.asScala.toList
  }

  def withFilePrefix[B](
      prefix: Seq[String]
  )(fun: ComputationEnvironment => B): B =
    fun(
      new ComputationEnvironment(
        resourceAllocated = resourceAllocated,
        components = components.withChildPrefix(prefix),
        launcher = launcher,
        taskActor = taskActor,
        taskHash = taskHash
      )
    )

  implicit def fileServiceComponent: FileServiceComponent = components.fs

  implicit def filePrefix: FileServicePrefix = components.filePrefix

  implicit def queue: Queue = components.queue

  implicit def cache: TaskResultCache = components.cache

  def toTaskSystemComponents =
    components

}

private[tasks] class Task(
    inputDeserializer: Spore[AnyRef, AnyRef],
    outputSerializer: Spore[AnyRef, AnyRef],
    function: Spore[AnyRef, AnyRef],
    launcherActor: LauncherHandle,
    queue: Queue,
    fileServiceComponent: FileServiceComponent,
    cache: TaskResultCache,
    nodeLocalCache: NodeLocalCache.State,
    resourceAllocated: ResourceAllocated,
    fileServicePrefix: FileServicePrefix,
    tasksConfig: TasksConfig,
    priority: Priority,
    labels: Labels,
    taskId: TaskId,
    lineage: TaskLineage,
    taskHash: HashedTaskDescription,
    val proxy: tasks.util.message.Address,
    messenger: Messenger
) {

  private def handleError(exception: Throwable) = {
    exception.printStackTrace()
    scribe.error(exception, "Task failed.")
    launcherActor.internalMessageTaskFailed(this, exception)
  }

  private def handleCompletion(
      program: IO[(UntypedResult, DependenciesAndRuntimeMetadata)],
      startTimeStamp: Instant,
      noCache: Boolean
  ) =
    program.attempt.flatMap {
      case Right((result, dependencies)) =>
        scribe.debug("Task success. ")
        val endTimeStamp = Instant.now
        launcherActor.internalMessageFromTask(
          this,
          UntypedResultWithMetadata(
            result,
            ResultMetadata(
              dependencies.dependencies,
              started = startTimeStamp,
              ended = endTimeStamp,
              logs = dependencies.logs,
              lineage = lineage
            ),
            noCache = noCache
          )
        )

      case Left(error) => handleError(error)
    }

  private def executeTaskProgram(
      input: Base64Data
  ): IO[(UntypedResult, DependenciesAndRuntimeMetadata)] =
    try {
      val history = HistoryContextImpl(
        task = History.TaskVersion(taskId.id, taskId.version),
        codeVersion = tasksConfig.codeVersion.s,
        traceId = Some(lineage.leaf.toString)
      )
      val ce = new ComputationEnvironment(
        resourceAllocated = resourceAllocated,
        components = new TaskSystemComponents(
          queue = queue,
          fs = fileServiceComponent,
          cache = cache,
          nodeLocalCache = nodeLocalCache,
          filePrefix = fileServicePrefix,
          tasksConfig = tasksConfig,
          historyContext = history,
          priority = priority,
          labels = labels,
          lineage = lineage,
          messenger = messenger
        ),
        launcher = launcherActor,
        taskActor = this,
        taskHash = taskHash
      )

      scribe.debug(
        s"Starting task with computation environment $ce and with input data $input."
      )

      IO.unit.flatMap { _ =>
        runUntypedTask[AnyRef, AnyRef](
          inputDeserializer.as[Unit, Deserializer[AnyRef]],
          outputSerializer.as[Unit, Serializer[AnyRef]],
          function.as[AnyRef, ComputationEnvironment => IO[AnyRef]],
          input
        )(ce)
      }
    } catch {
      case exception: Exception =>
        IO.raiseError(exception)
      case assertionError: AssertionError =>
        IO.raiseError(assertionError)
    }

  def start(input: MessageData.InputData): IO[Unit] = {
    import MessageData.InputData
    val InputData(b64InputData, noCache) = input

    handleCompletion(
      executeTaskProgram(b64InputData),
      startTimeStamp = java.time.Instant.now,
      noCache = noCache
    )

  }

  private def runUntypedTask[A, C](
      rs: Spore[Unit, Deserializer[A]],
      ws: Spore[Unit, Serializer[C]],
      fs: Spore[A, ComputationEnvironment => IO[C]],
      j: Base64Data
  ) =
    (ce: ComputationEnvironment) =>
      IO.interruptible {
        val r = rs(())
        val w = ws(())

        val deserialized = r(Base64DataHelpers.toBytes(j)) match {
          case Right(value) => value
          case Left(error) =>
            val logMessage =
              s"Could not deserialize input. Error: $error. Raw data (as utf8): ${new String(Base64DataHelpers.toBytes(j))}"
            scribe.error(logMessage)
            throw new RuntimeException(logMessage)
        }
        (w, deserialized)
      }.flatMap { case (w, deserializedInputData) =>
        fs(deserializedInputData)(ce).flatMap { result =>
          tasks.queue
            .extractDataDependencies(deserializedInputData)(ce)
            .map { meta =>
              (UntypedResult.make(result)(w), meta)
            }
        }
      }
}
