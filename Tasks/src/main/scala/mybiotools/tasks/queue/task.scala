/*
* The MIT License
*
* Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
* Group Fellay
* Copyright (c) 2016 Istvan Bartha
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

import akka.actor.{ Actor, PoisonPill, ActorRef, ActorContext, ActorRefFactory }
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

case class ProxyTaskActorRef[B <: Prerequisitive[B], T <: Result](private val actor: ActorRef) {
  def ~>[C <: Prerequisitive[C], A <: Result](child: ProxyTaskActorRef[C, A])(implicit updater: UpdatePrerequisitive[C, T]): ProxyTaskActorRef[C, A] = {
    ProxyTask.addTarget(actor, child.actor, updater)
    child
  }
  def ?(implicit ec: ExecutionContext) = ProxyTask.getBackResultFuture(actor).asInstanceOf[Future[T]]
  def ?!(implicit ec: ExecutionContext) = ProxyTask.getBackResult(actor).asInstanceOf[T]

  def <~[A <: Result](result: A)(implicit updater: UpdatePrerequisitive[B, A]) {
    ProxyTask.sendStartData(actor, List(result))
  }

}

// This is the output of a task
trait Result extends Serializable {
  def verifyAfterCache(implicit service: FileServiceActor, context: ActorRefFactory): Boolean = true
}

abstract class ResultWithSharedFiles(sf: SharedFile*) extends Result with Product {
  def files = sf
  override def verifyAfterCache(implicit service: FileServiceActor, context: ActorRefFactory) = files.forall(_.isAccessible) && productIterator.filter(_.isInstanceOf[ResultWithSharedFiles]).forall(_.asInstanceOf[ResultWithSharedFiles].verifyAfterCache)
}

// This is the prerequisitives of a task
trait Prerequisitive[+A] extends Serializable {
  def ready: Boolean

  def persistent: Prerequisitive[A] = this
}

trait SimplePrerequisitive[+A] extends Prerequisitive[A] with Product { self =>

  def ready = productIterator.forall {
    case x: Option[_] => x.isDefined
    case _ => throw new RuntimeException("SimplePrerequisitive should be a product of Options")
  }
}

@SerialVersionUID(1L)
case class NodeLocalCacheActor(actor: ActorRef) extends Serializable

@SerialVersionUID(1L)
case class QueueActor(actor: ActorRef) extends Serializable

@SerialVersionUID(1L)
case class LauncherActor(actor: ActorRef) extends Serializable

case class HostForMPI(hostname: String, slots: Int)

object LauncherActor {
  def block[T](request: CPUMemoryRequest)(k: => T)(implicit l: LauncherActor) = {
    l.actor ! BlockOn(request)
    val x = k
    l.actor ! BlockOff(request)
    k
  }
}

case class ComputationEnvironment(
    val resourceAllocated: CPUMemoryAllocated,
    val availableHostsForMPI: Seq[HostForMPI],
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

  def mpiHostFile = {
    val tmp = TempFile.createTempFile(".txt")
    writeToFile(tmp, availableHostsForMPI.map(x => s"${x.hostname} slots=${x.slots}").mkString("\n"))
    tmp
  }

  def toTaskSystemComponents =
    components

}

case class AddTarget[A <: Prerequisitive[A], B <: Result](target: ActorRef, updater: UpdatePrerequisitive[A, B])
case class AddTargetNoCheck(target: ActorRef)
case class WhatAreYourChildren[A <: Prerequisitive[A], B <: Result](
  notification: ActorRef,
  updater: UpdatePrerequisitive[A, B]
)

object ProxyTask {

  def getBackResultFuture(actor: ActorRef, timeoutp: Int = config.ProxyTaskGetBackResult)(implicit ec: ExecutionContext): Future[Result] = {

    implicit val timout = akka.util.Timeout(timeoutp seconds)
    (actor ? (GetBackResult)).asInstanceOf[Future[Result]]

  }

  def getBackResult(actor: ActorRef, timeoutp: Int = config.ProxyTaskGetBackResult)(implicit ec: ExecutionContext): Result = scala.concurrent.Await.result(getBackResultFuture(actor, timeoutp), timeoutp second)

  def addTarget[B <: Prerequisitive[B], A <: Result](parent: ActorRef, child: ActorRef, updater: UpdatePrerequisitive[B, A]) {

    val f = parent ! AddTarget(child, updater)
  }

  def sendStartData(target: ActorRef, stuff: Seq[Result]) {

    stuff.foreach { x =>
      (target ! x)
    }
  }

  def sendStartDataWithRetry(target: ActorRef, stuff: Seq[Result]) {
    sendStartData(target, stuff)

  }

}

private class Task[P <: Prerequisitive[P], Q <: Result](
    runTask: CompFun[P, Q],
    launcherActor: ActorRef,
    balancerActor: ActorRef,
    fileServiceActor: ActorRef,
    globalCacheActor: ActorRef,
    nodeLocalCache: ActorRef,
    resourceAllocated: CPUMemoryAllocated,
    hostsForMPI: Seq[HostForMPI],
    fileServicePrefix: FileServicePrefix
) extends Actor with akka.actor.ActorLogging {

  override def preStart {
    log.debug("Prestart of Task class")
  }

  override def postStop {
    log.debug("Task stopped.")
  }

  private[this] var done = false
  private[this] var started = false
  private[this] var notificationRegister: List[ActorRef] = List[ActorRef]()
  private[this] val mainActor = this.self
  private var resultG: Result = null

  private def startTask(msg: P) {
    started = true

    Future {
      try {
        log.debug("Starttask from the executing dispatcher (future).")

        val ce = ComputationEnvironment(
          resourceAllocated,
          hostsForMPI,
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

        log.debug("Task job ended. sending to launcher. from the executing dispatcher (future).")

        log.debug("Sending results over to proxies, etc: " + notificationRegister.toString)

        notificationRegister.foreach(_ ! MessageFromTask(resultG))

        launcherActor ! InternalMessageFromTask(mainActor, result)

        log.debug("Task ended. Result sent to launcherActor. Taking PoisonPill")

        self ! PoisonPill

      } catch {
        case x: Exception => {
          val y = x.getStackTrace
          x.printStackTrace()
          log.error(x, "Exception caught in the executing dispatcher of a task. " + x.getMessage)
          launcherActor ! InternalMessageTaskFailed(mainActor, x)
          self ! PoisonPill
        }
        case x: AssertionError => {
          val y = x.getStackTrace
          x.printStackTrace()
          log.error(x, "Exception caught in the executing dispatcher of a task. " + x.getMessage)
          launcherActor ! InternalMessageTaskFailed(mainActor, x)
          self ! PoisonPill
        }
      }
    }(context.dispatcher)
  }

  def receive = {
    case msg: Prerequisitive[_] => {
      log.debug("StartTask, from taskactor")
      startTask(msg.asInstanceOf[P])
    }
    case RegisterForNotification(ac) => {
      log.debug("Received: " + ac.toString)
      notificationRegister = ac :: notificationRegister
    }
    case x => log.debug("received unknown message" + x)
  }
}

abstract class ProxyTask(
    starter: ActorRef,
    fileServiceActor: ActorRef,
    fileServicePrefix: FileServicePrefix,
    cacheActor: ActorRef
) extends Actor with akka.actor.ActorLogging {

  protected type MyPrerequisitive <: Prerequisitive[MyPrerequisitive]

  protected type MyResult <: Result

  protected def resourceConsumed = CPUMemoryRequest(cpu = 1, memory = 500)

  def handleIncomingResult(r: Result) {
    incomings = _updatePrerequisitives.foldLeft(identity[MyPrerequisitive])((b, a) => UpdatePrerequisitive(a orElse b)).apply((incomings, r))
  }

  val runTaskClass: java.lang.Class[_ <: CompFun[MyPrerequisitive, MyResult]]

  val taskID: String

  private[this] var _targets: Set[ActorRef] = Set[ActorRef]()

  private[this] var _channels: Set[ActorRef] = Set[ActorRef]()

  def emptyResultSet: MyPrerequisitive

  private[this] var _updatePrerequisitives = List[UpdatePrerequisitive[MyPrerequisitive, Result]]()

  protected var incomings: MyPrerequisitive = emptyResultSet

  private[this] var result: Option[Result] = None

  private var taskIsQueued = false

  private var targetNegotiation = 0

  private def distributeResult {
    log.debug("Distributing result to targets: " + _targets.toString + ", " + _channels.toString)
    result.foreach(r => _targets.foreach { t => t ! r })
    result.foreach(r => _channels.foreach { ch => ch ! r })
  }

  private def notifyListenersOnFailure(cause: Throwable) {
    _targets.foreach(t => t ! FailureMessageFromProxyToProxy(cause))
    _channels.foreach(t => t ! akka.actor.Status.Failure(cause))
  }

  private def startTask: Unit = {
    if (result.isEmpty) {

      val s = ScheduleTask(
        TaskDescription(taskID, incomings), runTaskClass.getName, resourceConsumed,
        starter, // new ActorInEnvelope(starter),
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
    case x: AddTarget[_, _] => {
      if (x.target == self) sender ! false
      else {
        targetNegotiation += 1
        x.target ! WhatAreYourChildren(sender, x.updater)
      }

    }
    case x: WhatAreYourChildren[_, _] => {
      log.debug("whatareyourchildren")
      if (targetNegotiation > 0) {
        self forward WhatAreYourChildren(x.notification, x.updater)
      } else {
        sender ! ChildrenMessage(_targets, x.notification, x.updater)
      }
    }
    case x: ChildrenMessage[_, _] => {
      val newtarget = sender

      if (newtarget == self || x.mytargets.contains(self)) {
        log.error("Adding " + newtarget.toString + " to the dependency graph would introduce a cycle in the graph.")
        targetNegotiation -= 1
        x.notification ! false
      } else {
        sender ! SaveUpdater(x.updater)
        x.notification ! true
      }

    }
    case x: SaveUpdater[_, _] => {
      _updatePrerequisitives = x.updater.asInstanceOf[UpdatePrerequisitive[MyPrerequisitive, Result]] :: _updatePrerequisitives
      sender ! UpdaterSaved
    }
    case UpdaterSaved => {
      targetNegotiation -= 1

      log.debug("target added")
      _targets = _targets + sender
      result.foreach(r => sender ! r)
    }
    case msg: Result => {
      log.debug("Message received, handling in handleIncomingResult. ")
      handleIncomingResult(msg)
      if (incomings.ready && !taskIsQueued) startTask
    }
    case MessageFromTask(incomingResult) => {
      log.debug("Message received from: " + sender.toString + ", ")
      if (result.isEmpty) {
        result = Some(incomingResult)
        distributeResult
      }
    }
    case GetBackResult => {
      log.debug("GetBackResult message received. Registering for notification: " + sender.toString)
      _channels = _channels + sender //.asInstanceOf[Channel[Result]]
      distributeResult
    }
    case ATaskWasForwarded => {
      log.debug("The loadbalancer received the message and queued it.")
      taskIsQueued = true
    }
    case TaskFailedMessageToProxy(sch, cause) => {
      log.error(cause, "Execution failed. ")
      notifyListenersOnFailure(cause)
      self ! PoisonPill
    }
    case FailureMessageFromProxyToProxy(cause) => {
      notifyListenersOnFailure(cause)
      self ! PoisonPill
    }
    case msg => log.warning("Unhandled message " + msg.toString.take(100) + sender.toString)
  }

}
