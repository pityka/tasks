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
import akka.stream.Materializer

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.util._

import tasks.fileservice._
import tasks.util.config._
import tasks.shared._
import tasks._
import tasks.wire._

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._

case class UntypedResult(files: Set[SharedFile], data: Base64Data)

object UntypedResult {

  private def files(r: Any): Set[SharedFile] = r match {
    case resultWithSharedFiles: HasSharedFiles =>
      resultWithSharedFiles.files.toSet ++ resultWithSharedFiles.productIterator
        .flatMap(member => files(member))
        .toSet
    case _ => Set()
  }

  def make[A](r: A)(implicit ser: Serializer[A]): UntypedResult =
    UntypedResult(files(r), Base64DataHelpers(ser(r)))

  implicit val encoder: Encoder[UntypedResult] = deriveEncoder[UntypedResult]

  implicit val decoder: Decoder[UntypedResult] = deriveDecoder[UntypedResult]
}

case class ComputationEnvironment(
    val resourceAllocated: ResourceAllocated,
    implicit val components: TaskSystemComponents,
    implicit val log: akka.event.LoggingAdapter,
    implicit val launcher: LauncherActor,
    implicit val executionContext: ExecutionContext,
    val taskActor: ActorRef
) {

  def withFilePrefix[B](prefix: Seq[String])(
      fun: ComputationEnvironment => B): B =
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
    runTask: CompFun2,
    launcherActor: ActorRef,
    queueActor: ActorRef,
    fileServiceComponent: FileServiceComponent,
    cacheActor: ActorRef,
    nodeLocalCache: ActorRef,
    resourceAllocated: ResourceAllocated,
    fileServicePrefix: FileServicePrefix,
    auxExecutionContext: ExecutionContext,
    actorMaterializer: Materializer,
    tasksConfig: TasksConfig
) extends Actor
    with akka.actor.ActorLogging {

  override def preStart: Unit = {
    log.debug("Prestart of Task class")
  }

  override def postStop: Unit = {
    fjp.shutdown
    log.debug(s"Task stopped. Input was: $input.")

  }

  private var notificationRegister: List[ActorRef] = List[ActorRef]()

  val fjp = tasks.util.concurrent
    .newJavaForkJoinPoolWithNamePrefix("tasks-ec", resourceAllocated.cpu)
  val executionContextOfTask =
    scala.concurrent.ExecutionContext.fromExecutorService(fjp)

  private var input: Option[String] = None

  private def handleError(exception: Throwable): Unit = {
    exception.printStackTrace()
    log.error(exception, "Task failed.")
    launcherActor ! InternalMessageTaskFailed(self, exception)
    self ! PoisonPill
  }

  private def handleCompletion(future: Future[UntypedResult]) =
    future.onComplete {
      case Success(result) =>
        log.debug("Task success. Notifications: {}",
                  notificationRegister.toString)
        notificationRegister.foreach(_ ! MessageFromTask(result))
        launcherActor ! InternalMessageFromTask(self, result)
        self ! PoisonPill

      case Failure(error) => handleError(error)
    }(executionContextOfTask)

  private def executeTaskAsynchronously(
      msg: Base64Data): Future[UntypedResult] =
    try {
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
          actorMaterializer,
          tasksConfig,
          NoHistory
        ),
        akka.event.Logging(context.system.eventStream,
                           "usertasks." + fileServicePrefix.list.mkString(".")),
        LauncherActor(launcherActor),
        executionContextOfTask,
        self
      )

      log.debug(
        s"Starting task with computation environment $ce and with input data $msg.")

      Future(runTask(msg)(ce))(executionContextOfTask)
        .flatMap(identity)(executionContextOfTask)
    } catch {
      case exception: Exception =>
        Future.failed(exception)
      case assertionError: AssertionError =>
        Future.failed(assertionError)
    }

  def receive = {
    case bd: Base64Data =>
      log.debug("Received input data.")
      input = Some(bd.value)
      handleCompletion(executeTaskAsynchronously(bd))

    case RegisterForNotification(actorRef) =>
      log.debug("Registering listener: " + actorRef.toString)
      notificationRegister = actorRef :: notificationRegister

    case other => log.error("received unknown message" + other)
  }
}
