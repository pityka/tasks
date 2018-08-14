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
import akka.util.Timeout
import akka.pattern.ask
import akka.stream.Materializer

import scala.concurrent.duration._
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

case class TaskId(id: String, version: Int)
object TaskId {
  implicit val encoder: Encoder[TaskId] = deriveEncoder[TaskId]
  implicit val dec: Decoder[TaskId] = deriveDecoder[TaskId]
}

object UntypedResult {

  def fs(r: Any): Set[SharedFile] = r match {
    case x: ResultWithSharedFiles =>
      x.files.toSet ++ x.productIterator.flatMap(x => fs(x)).toSet
    case x: SharedFile => Set(x)
    case _             => Set()
  }

  def make[A](r: A)(implicit ser: Serializer[A]): UntypedResult =
    UntypedResult(fs(r), Base64Data(ser(r)))

  implicit val encoder: Encoder[UntypedResult] = deriveEncoder[UntypedResult]

  implicit val decoder: Decoder[UntypedResult] = deriveDecoder[UntypedResult]
}

case class ComputationEnvironment(
    val resourceAllocated: CPUMemoryAllocated,
    implicit val components: TaskSystemComponents,
    implicit val log: akka.event.LoggingAdapter,
    implicit val launcher: LauncherActor,
    implicit val executionContext: ExecutionContext,
    val taskActor: ActorRef
) {

  implicit def fs: FileServiceActor = components.fs

  implicit def actorsystem: akka.actor.ActorSystem = components.actorsystem

  implicit def filePrefix: FileServicePrefix = components.filePrefix

  implicit def nodeLocalCache: NodeLocalCacheActor = components.nodeLocalCache

  implicit def queue: QueueActor = components.queue

  implicit def cache: CacheActor = components.cache

  def toTaskSystemComponents =
    components

}

private[tasks] object ProxyTask {

  def getBackResultFuture(actor: ActorRef,
                          timeoutp: FiniteDuration): Future[Any] = {

    implicit val timout = Timeout(timeoutp)
    (actor ? (GetBackResult))

  }

}

private class Task(
    runTask: CompFun2,
    launcherActor: ActorRef,
    balancerActor: ActorRef,
    fileServiceActor: FileServiceActor,
    globalCacheActor: ActorRef,
    nodeLocalCache: ActorRef,
    resourceAllocated: CPUMemoryAllocated,
    fileServicePrefix: FileServicePrefix,
    auxExecutionContext: ExecutionContext,
    actorMaterializer: Materializer,
    tasksConfig: TasksConfig
) extends Actor
    with akka.actor.ActorLogging {

  override def preStart {
    log.debug("Prestart of Task class")
  }

  override def postStop {
    fjp.shutdown
    log.debug("Task stopped, {}", startdat)

  }

  var notificationRegister: List[ActorRef] = List[ActorRef]()
  val mainActor = this.self

  val fjp = tasks.util.concurrent
    .newJavaForkJoinPoolWithNamePrefix("tasks-ec", resourceAllocated.cpu)
  val executionContext =
    scala.concurrent.ExecutionContext.fromExecutorService(fjp)

  var startdat: Option[String] = None

  def startTask(msg: Base64Data): Unit =
    try {
      log.debug("Starttask from the executing dispatcher (future).")

      val ce = ComputationEnvironment(
        resourceAllocated,
        TaskSystemComponents(
          QueueActor(balancerActor),
          fileServiceActor,
          context.system,
          CacheActor(globalCacheActor),
          NodeLocalCacheActor(nodeLocalCache),
          fileServicePrefix,
          auxExecutionContext,
          actorMaterializer,
          tasksConfig
        ),
        akka.event.Logging(context.system.eventStream,
                           "usertasks." + fileServicePrefix.list.mkString(".")),
        LauncherActor(launcherActor),
        executionContext,
        self
      )

      log.debug("CE {}", ce)
      log.debug("start data {}", msg)

      val f = Future(runTask(msg)(ce))(executionContext)
        .flatMap(x => x)(executionContext)

      f.onComplete {
        case Success(result) =>
          log.debug("Task success. Notifications: {}",
                    notificationRegister.toString)
          notificationRegister.foreach(_ ! MessageFromTask(result))
          launcherActor ! InternalMessageFromTask(mainActor, result)
          self ! PoisonPill

        case Failure(x) =>
          x.printStackTrace()
          log.error(
            x,
            "Exception caught in the executing dispatcher of a task. " + x.getMessage)
          launcherActor ! InternalMessageTaskFailed(mainActor, x)
          self ! PoisonPill

      }(executionContext)

    } catch {
      case x: Exception => {
        x.printStackTrace()
        log.error(
          x,
          "Exception caught in the executing dispatcher of a task. " + x.getMessage)
        launcherActor ! InternalMessageTaskFailed(mainActor, x)
        self ! PoisonPill
      }
      case x: AssertionError => {
        x.printStackTrace()
        log.error(
          x,
          "Exception caught in the executing dispatcher of a task. " + x.getMessage)
        launcherActor ! InternalMessageTaskFailed(mainActor, x)
        self ! PoisonPill
      }
    }

  def receive = {
    case bd: Base64Data =>
      log.debug("StartTask, from taskactor")
      startdat = Some(bd.value)
      startTask(bd)
    // startTask(io.circe.parser.parse(msg).right.get)

    case RegisterForNotification(ac) =>
      log.debug("Received: " + ac.toString)
      notificationRegister = ac :: notificationRegister

    case x => log.error("received unknown message" + x)
  }
}

class ProxyTask[MyPrerequisitive, MyResult](
    taskId: TaskId,
    runTaskClass: java.lang.Class[_ <: CompFun2],
    incomings: MyPrerequisitive,
    writer: Serializer[MyPrerequisitive],
    reader: Deserializer[MyResult],
    resourceConsumed: VersionedCPUMemoryRequest,
    starter: ActorRef,
    fileServiceActor: FileServiceActor,
    fileServicePrefix: FileServicePrefix,
    cacheActor: ActorRef
) extends Actor
    with akka.actor.ActorLogging {

  private[this] var _channels: Set[ActorRef] = Set[ActorRef]()

  private[this] var result: Option[Any] = None

  private var taskIsQueued = false

  private def distributeResult(): Unit = {
    log.debug("Distributing result to targets: {}", _channels)
    result.foreach(r =>
      _channels.foreach { ch =>
        ch ! r
    })
  }

  private def notifyListenersOnFailure(cause: Throwable): Unit =
    _channels.foreach(t => t ! akka.actor.Status.Failure(cause))

  private def startTask(): Unit = {
    if (result.isEmpty) {

      val persisted: Option[MyPrerequisitive] = incomings match {
        case x: HasPersistent[MyPrerequisitive] => Some(x.persistent)
        case _                                  => None
      }

      val s = ScheduleTask(
        TaskDescription(taskId,
                        Base64Data(writer(incomings)),
                        persisted.map(x => Base64Data(writer(x)))),
        runTaskClass.getName,
        resourceConsumed,
        starter,
        fileServiceActor.actor,
        fileServicePrefix,
        cacheActor
      )

      log.debug("proxy submitting ScheduleTask object to queue.")

      starter ! s
    }
  }

  override def preStart() = {
    log.debug("ProxyTask prestart.")
    startTask

  }

  override def postStop() = {
    log.debug("ProxyTask stopped. {} {} {}", taskId, incomings, self)
  }

  def receive = {
    case MessageFromTask(incomingResultRaw) =>
      val incomingResult: MyResult = reader(incomingResultRaw.data.bytes)
      // reader
      //   .decodeJson(
      //     io.circe.parser.parse(incomingResultJs.data.value).right.get)
      //   .right
      //   .get
      log.debug("MessageFromTask received from: {}, {}, {},{}",
                sender,
                incomingResultRaw,
                result,
                incomingResult)
      if (result.isEmpty) {
        result = Some(incomingResult)
        distributeResult
      }

    case GetBackResult =>
      log.debug(
        "GetBackResult message received. Registering for notification: " + sender.toString)
      _channels = _channels + sender //.asInstanceOf[Channel[Result]]
      distributeResult

    case ATaskWasForwarded =>
      if (!taskIsQueued) {
        log.debug("The loadbalancer received the message and queued it.")
        taskIsQueued = true
      }

    case TaskFailedMessageToProxy(_, cause) =>
      log.error(cause, "Execution failed. ")
      notifyListenersOnFailure(cause)
      self ! PoisonPill
  }

}
