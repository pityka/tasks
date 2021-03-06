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

import akka.actor.{Actor, PoisonPill, ActorRef}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.util._

import tasks.fileservice._
import tasks.util.config._
import tasks.shared._
import tasks._
import tasks.wire._

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

import java.time.Instant

case class UntypedResult(
    files: Set[SharedFile],
    data: Base64Data,
    mutableFiles: Option[Set[SharedFile]]
)

case class DependenciesAndRuntimeMetadata(
    dependencies: Seq[History],
    logs: Seq[LogRecord]
)

object DependenciesAndRuntimeMetadata {
  implicit val codec: JsonValueCodec[DependenciesAndRuntimeMetadata] =
    JsonCodecMaker.make

}

case class TaskInvocationId(id: TaskId, description: HashedTaskDescription) {
  override def toString = id.id + "_" + description.hash
}
object TaskInvocationId {
  implicit val codec: JsonValueCodec[TaskInvocationId] = JsonCodecMaker.make
}

case class TaskLineage(lineage: Seq[TaskInvocationId]) {
  def leaf = lineage.last
  def inherit(td: HashedTaskDescription) =
    TaskLineage(
      lineage :+ TaskInvocationId(td.taskId, td)
    )
}

object TaskLineage {
  def root = TaskLineage(Seq.empty)
  implicit val codec: JsonValueCodec[TaskLineage] = JsonCodecMaker.make
}

case class ResultMetadata(
    dependencies: Seq[History],
    started: Instant,
    ended: Instant,
    logs: Seq[LogRecord],
    lineage: TaskLineage
)

object ResultMetadata {
  implicit val codec: JsonValueCodec[ResultMetadata] = JsonCodecMaker.make
}

case class UntypedResultWithMetadata(
    untypedResult: UntypedResult,
    metadata: ResultMetadata
)

object UntypedResultWithMetadata {
  implicit val codec: JsonValueCodec[UntypedResultWithMetadata] =
    JsonCodecMaker.make
}

object UntypedResult {

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
    JsonCodecMaker.make(CodecMakerConfig.withSetMaxInsertNumber(2147483647))
}

case class ComputationEnvironment(
    val resourceAllocated: ResourceAllocated,
    implicit val components: TaskSystemComponents,
    implicit val log: akka.event.LoggingAdapter,
    implicit val launcher: LauncherActor,
    implicit val executionContext: ExecutionContext,
    val taskActor: ActorRef,
    taskHash: HashedTaskDescription
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
    fun(copy(components = components.withChildPrefix(prefix)))

  implicit def fileServiceComponent: FileServiceComponent = components.fs

  implicit def actorSystem: akka.actor.ActorSystem = components.actorsystem

  implicit def filePrefix: FileServicePrefix = components.filePrefix

  implicit def nodeLocalCache: NodeLocalCacheActor = components.nodeLocalCache

  implicit def queue: QueueActor = components.queue

  implicit def cache: CacheActor = components.cache

  def toTaskSystemComponents =
    components

}

private class Task(
    inputDeserializer: Spore[AnyRef, AnyRef],
    outputSerializer: Spore[AnyRef, AnyRef],
    function: Spore[AnyRef, AnyRef],
    launcherActor: ActorRef,
    queueActor: ActorRef,
    fileServiceComponent: FileServiceComponent,
    cacheActor: ActorRef,
    nodeLocalCache: ActorRef,
    resourceAllocated: ResourceAllocated,
    fileServicePrefix: FileServicePrefix,
    auxExecutionContext: ExecutionContext,
    tasksConfig: TasksConfig,
    priority: Priority,
    labels: Labels,
    taskId: TaskId,
    lineage: TaskLineage,
    taskHash: HashedTaskDescription,
    proxy: ActorRef
) extends Actor
    with akka.actor.ActorLogging {

  override def preStart(): Unit = {
    log.debug("Prestart of Task class")
    proxy ! NeedInput
  }

  override def postStop(): Unit = {
    fjp.shutdown
    log.debug(s"Task stopped. $taskId")
  }

  val fjp = tasks.util.concurrent
    .newJavaForkJoinPoolWithNamePrefix("tasks-ec", resourceAllocated.cpu)
  val executionContextOfTask =
    scala.concurrent.ExecutionContext.fromExecutorService(fjp)

  private def handleError(exception: Throwable): Unit = {
    exception.printStackTrace()
    log.error(exception, "Task failed.")
    launcherActor ! InternalMessageTaskFailed(self, exception)
    self ! PoisonPill
  }

  private def handleCompletion(
      future: Future[(UntypedResult, DependenciesAndRuntimeMetadata)],
      startTimeStamp: Instant
  ) =
    future.onComplete {
      case Success((result, dependencies)) =>
        log.debug("Task success. ")
        val endTimeStamp = Instant.now
        launcherActor ! InternalMessageFromTask(
          self,
          UntypedResultWithMetadata(
            result,
            ResultMetadata(
              dependencies.dependencies,
              started = startTimeStamp,
              ended = endTimeStamp,
              logs = dependencies.logs,
              lineage = lineage
            )
          )
        )
        self ! PoisonPill

      case Failure(error) => handleError(error)
    }(executionContextOfTask)

  private def executeTaskAsynchronously(
      input: Base64Data
  ): Future[(UntypedResult, DependenciesAndRuntimeMetadata)] =
    try {
      val history = HistoryContextImpl(
        task = History.TaskVersion(taskId.id, taskId.version),
        codeVersion = tasksConfig.codeVersion,
        traceId = Some(lineage.leaf.toString)
      )
      val ce = ComputationEnvironment(
        resourceAllocated,
        TaskSystemComponents(
          QueueActor(queueActor),
          fileServiceComponent,
          context.system,
          CacheActor(cacheActor),
          NodeLocalCacheActor(nodeLocalCache),
          fileServicePrefix,
          auxExecutionContext,
          tasksConfig,
          history,
          priority,
          labels,
          lineage
        ),
        akka.event.Logging(
          context.system.eventStream,
          "usertasks." + fileServicePrefix.list.mkString(".")
        ),
        LauncherActor(launcherActor),
        executionContextOfTask,
        self,
        taskHash
      )

      log.debug(
        s"Starting task with computation environment $ce and with input data $input."
      )

      Future {
        val untyped =
          UntypedTaskDefinition[AnyRef, AnyRef](
            inputDeserializer.as[Unit, Deserializer[AnyRef]],
            outputSerializer.as[Unit, Serializer[AnyRef]],
            function.as[AnyRef, ComputationEnvironment => Future[AnyRef]]
          )
        untyped.apply(input)(ce)
      }(executionContextOfTask)
        .flatMap(identity)(executionContextOfTask)
    } catch {
      case exception: Exception =>
        Future.failed(exception)
      case assertionError: AssertionError =>
        Future.failed(assertionError)
    }

  def receive = {
    case InputData(b64InputData) =>
      handleCompletion(
        executeTaskAsynchronously(b64InputData),
        startTimeStamp = java.time.Instant.now
      )

    case other => log.error("received unknown message" + other)
  }
}
