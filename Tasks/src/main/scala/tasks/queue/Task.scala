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

import akka.actor.{Actor, PoisonPill, ActorRef, ActorContext, ActorRefFactory}
import akka.util.Timeout
import akka.pattern.ask

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

import java.io.File

import tasks.fileservice._
import tasks.util._
import tasks.shared._
import tasks._
import tasks.caching._

import upickle.Js
import upickle.default._

case class UntypedResult(files: Set[SharedFile], data: JsonString)

object UntypedResult {

  def fs(r: Any): Set[SharedFile] = r match {
    case x: ResultWithSharedFiles =>
      x.files.toSet ++ x.productIterator.flatMap(x => fs(x)).toSet
    case _ => Set()
  }

  def make[A: Writer](r: A): UntypedResult =
    UntypedResult(fs(r), JsonString(write(r)))

}

case class ComputationEnvironment(
    val resourceAllocated: CPUMemoryAllocated,
    implicit val components: TaskSystemComponents,
    implicit val log: akka.event.LoggingAdapter,
    implicit val launcher: LauncherActor
) extends Serializable {

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
                          timeoutp: FiniteDuration =
                            config.global.proxyTaskGetBackResult)(
      implicit ec: ExecutionContext): Future[Any] = {

    implicit val timout = Timeout(timeoutp)
    (actor ? (GetBackResult))

  }

  def getBackResult(actor: ActorRef,
                    timeoutp: FiniteDuration =
                      config.global.proxyTaskGetBackResult)(
      implicit ec: ExecutionContext): Any =
    scala.concurrent.Await
      .result(getBackResultFuture(actor, timeoutp), timeoutp)

  def addTarget[B <: Prerequisitive[B], A](
      parent: ActorRef,
      child: ActorRef,
      updater: UpdatePrerequisitive[B, A]) {

    val f = parent ! AddTarget(child, updater)
  }

}

private class Task(
    runTask: CompFun2,
    launcherActor: ActorRef,
    balancerActor: ActorRef,
    fileServiceActor: ActorRef,
    globalCacheActor: ActorRef,
    nodeLocalCache: ActorRef,
    resourceAllocated: CPUMemoryAllocated,
    fileServicePrefix: FileServicePrefix
) extends Actor
    with akka.actor.ActorLogging {

  override def preStart {
    log.debug("Prestart of Task class")
  }

  override def postStop {
    log.debug("Task stopped.")
  }

  private[this] var notificationRegister: List[ActorRef] = List[ActorRef]()
  private[this] val mainActor = this.self
  private var resultG: UntypedResult = null

  private def startTask(msg: Js.Value) {

    Future {
      try {
        log.debug("Starttask from the executing dispatcher (future).")

        val ce = ComputationEnvironment(
            resourceAllocated,
            TaskSystemComponents(
                QueueActor(balancerActor),
                FileServiceActor(fileServiceActor),
                context.system,
                CacheActor(globalCacheActor),
                NodeLocalCacheActor(nodeLocalCache),
                fileServicePrefix
            ),
            getApplicationLogger(runTask)(context.system),
            LauncherActor(launcherActor)
        )

        log.debug("CE" + ce)
        log.debug("start data " + msg)

        val result = runTask(msg)(ce)

        resultG = result

        log.debug(
            "Task job ended. sending to launcher. from the executing dispatcher (future).")

        log.debug(
            "Sending results over to proxies, etc: " + notificationRegister.toString)

        notificationRegister.foreach(_ ! MessageFromTask(resultG))

        launcherActor ! InternalMessageFromTask(mainActor, result)

        log.debug(
            "Task ended. Result sent to launcherActor. Taking PoisonPill")

        self ! PoisonPill

      } catch {
        case x: Exception => {
          val y = x.getStackTrace
          x.printStackTrace()
          log.error(
              x,
              "Exception caught in the executing dispatcher of a task. " + x.getMessage)
          launcherActor ! InternalMessageTaskFailed(mainActor, x)
          self ! PoisonPill
        }
        case x: AssertionError => {
          val y = x.getStackTrace
          x.printStackTrace()
          log.error(
              x,
              "Exception caught in the executing dispatcher of a task. " + x.getMessage)
          launcherActor ! InternalMessageTaskFailed(mainActor, x)
          self ! PoisonPill
        }
      }
    }(context.dispatcher)
  }

  def receive = {
    case JsonString(msg) =>
      log.debug("StartTask, from taskactor")
      startTask(upickle.json.read(msg))

    case RegisterForNotification(ac) =>
      log.debug("Received: " + ac.toString)
      notificationRegister = ac :: notificationRegister

    case x => log.error("received unknown message" + x)
  }
}

abstract class ProxyTask(
    starter: ActorRef,
    fileServiceActor: ActorRef,
    fileServicePrefix: FileServicePrefix,
    cacheActor: ActorRef
) extends Actor
    with akka.actor.ActorLogging {

  protected type MyPrerequisitive <: Prerequisitive[MyPrerequisitive]

  protected type MyResult

  protected def resourceConsumed =
    tasks.CPUMemoryRequest(cpu = 1, memory = 500)

  def handleIncomingResult(r: Any) {
    incomings = _updatePrerequisitives
      .foldLeft(identity[MyPrerequisitive, Any])((b, a) =>
            UpdatePrerequisitive(a orElse b))
      .apply((incomings, r))
  }

  val runTaskClass: java.lang.Class[_ <: CompFun2]

  val taskID: String

  private[this] var _targets: Set[ActorRef] = Set[ActorRef]()

  private[this] var _channels: Set[ActorRef] = Set[ActorRef]()

  def emptyResultSet: MyPrerequisitive

  val writer: Writer[MyPrerequisitive]

  val reader: Reader[MyResult]

  private[this] var _updatePrerequisitives =
    List[UpdatePrerequisitive[MyPrerequisitive, Any]]()

  protected var incomings: MyPrerequisitive = emptyResultSet

  private[this] var result: Option[Any] = None

  private var taskIsQueued = false

  private def distributeResult {
    log.debug(
        "Distributing result to targets: " + _targets.toString + ", " + _channels.toString)
    result.foreach(r =>
          _targets.foreach { t =>
        t ! r
    })
    result.foreach(r =>
          _channels.foreach { ch =>
        ch ! r
    })
  }

  private def notifyListenersOnFailure(cause: Throwable) {
    _targets.foreach(t => t ! FailureMessageFromProxyToProxy(cause))
    _channels.foreach(t => t ! akka.actor.Status.Failure(cause))
  }

  private def startTask: Unit = {
    if (result.isEmpty) {

      val s = ScheduleTask(
          TaskDescription(
              taskID,
              JsonString(upickle.json.write(writer.write(incomings))),
              incomings.persistent.map(x =>
                    JsonString(upickle.json.write(writer.write(x.self))))),
          runTaskClass.getName,
          resourceConsumed,
          starter,
          fileServiceActor,
          fileServicePrefix,
          cacheActor
      )

      log.debug("proxy submitting ScheduleTask object to queue.")

      starter ! s
    }
  }

  override def preStart() = {
    log.debug("ProxyTask prestart.")
    if (incomings.ready) {
      startTask
    }
  }

  override def postStop() = {
    log.debug("ProxyTask stopped.")
  }

  def receive = {
    case x: AddTarget[_, _] =>
      if (x.target == self) sender ! false
      else {
        x.target ! SaveUpdater(x.updater)
      }

    case x: SaveUpdater[_, _] =>
      _updatePrerequisitives = x.updater.asInstanceOf[UpdatePrerequisitive[
                MyPrerequisitive,
                Any]] :: _updatePrerequisitives
      sender ! UpdaterSaved

    case UpdaterSaved =>
      log.debug("target added")
      _targets = _targets + sender
      result.foreach(r => sender ! r)

    case MessageFromTask(incomingResultJs) =>
      val incomingResult: MyResult =
        reader.read(upickle.json.read(incomingResultJs.data.value))
      log.debug("Message received from: " + sender.toString + ", ")
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
      log.debug("The loadbalancer received the message and queued it.")
      taskIsQueued = true

    case TaskFailedMessageToProxy(sch, cause) =>
      log.error(cause, "Execution failed. ")
      notifyListenersOnFailure(cause)
      self ! PoisonPill

    case FailureMessageFromProxyToProxy(cause) => {
      notifyListenersOnFailure(cause)
      self ! PoisonPill
    }
    case msg: Any =>
      log.debug("Message received, handling in handleIncomingResult. ")
      handleIncomingResult(msg)
      if (incomings.ready && !taskIsQueued) startTask
  }

}
