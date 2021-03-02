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

import akka.actor.ActorSystem
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent._

import tasks.wire._
import tasks.queue._
import tasks.fileservice._
import tasks.util.config.TasksConfig
import tasks.deploy._
import tasks.shared.LogRecord

import scala.language.experimental.macros
import scala.language.implicitConversions

package object tasks {

  val SharedFile = tasks.fileservice.SharedFile

  type SharedFile = tasks.fileservice.SharedFile

  type CodeVersion = tasks.shared.CodeVersion
  val CodeVersion = tasks.shared.CodeVersion _

  type ResourceRequest = tasks.shared.VersionedResourceRequest

  def ResourceRequest(cpu: (Int, Int), memory: Int, scratch: Int, gpu: Int)(
      implicit codeVersion: CodeVersion
  ) =
    tasks.shared.VersionedResourceRequest(
      codeVersion,
      tasks.shared.ResourceRequest(cpu, memory, scratch, gpu)
    )
  def ResourceRequest(cpu: (Int, Int), memory: Int, scratch: Int)(
      implicit codeVersion: CodeVersion
  ) =
    tasks.shared.VersionedResourceRequest(
      codeVersion,
      tasks.shared.ResourceRequest(cpu, memory, scratch, 0)
    )

  def ResourceRequest(cpu: (Int, Int), memory: Int)(
      implicit codeVersion: CodeVersion
  ) =
    tasks.shared.VersionedResourceRequest(
      codeVersion,
      tasks.shared.ResourceRequest(cpu, memory, 1, 0)
    )

  def ResourceRequest(cpu: Int, memory: Int)(
      implicit codeVersion: CodeVersion
  ) =
    tasks.shared.VersionedResourceRequest(
      codeVersion,
      tasks.shared.ResourceRequest((cpu, cpu), memory, 1, 0)
    )

  def ResourceRequest(cpu: Int, memory: Int, scratch: Int)(
      implicit codeVersion: CodeVersion
  ) =
    tasks.shared.VersionedResourceRequest(
      codeVersion,
      tasks.shared.ResourceRequest(cpu, memory, scratch, 0)
    )

  implicit def tsc(implicit ts: TaskSystem): TaskSystemComponents =
    ts.components

  implicit def tasksConfig(
      implicit component: TaskSystemComponents
  ): TasksConfig =
    component.tasksConfig

  implicit def codeVersionFromTasksConfig(
      implicit c: TasksConfig
  ): CodeVersion = c.codeVersion

  implicit def fs(
      implicit component: TaskSystemComponents
  ): FileServiceComponent =
    component.fs

  implicit def executionContext(
      implicit env: ComputationEnvironment
  ): ExecutionContext =
    env.executionContext

  def releaseResources(implicit comp: ComputationEnvironment) =
    comp.launcher.actor.!(Release)(comp.taskActor)

  implicit def ts(
      implicit component: ComputationEnvironment
  ): TaskSystemComponents =
    component.components

  implicit def launcherActor(
      implicit component: ComputationEnvironment
  ): LauncherActor =
    component.launcher

  implicit def resourceAllocated(implicit component: ComputationEnvironment) =
    component.resourceAllocated

  implicit def log(implicit component: ComputationEnvironment) = component.log

  implicit class AwaitableFuture[T](future: Future[T]) {
    /* Await for a future indefinitely from inside a task
     *
     * This is a convenience method for having less Future chaining in task code
     * As the number of concurrently executed task is relatively low and each
     * task receives its own threadpool with at least one thread, the implementor
     * of the task can freely decide to block the thread executing the task.
     *
     * To make this safe the following method is provided which only compiles from
     * a task body.
     */
    def awaitIndefinitely(implicit ce: ComputationEnvironment) = {
      val _ = ce // suppressing unused warning
      Await.result(future, atMost = scala.concurrent.duration.Duration.Inf)
    }
  }

  def audit(data: String)(implicit component: ComputationEnvironment) =
    component.appendLog(LogRecord(data, java.time.Instant.now))

  def withTaskSystem[T](f: TaskSystemComponents => T): Option[T] =
    withTaskSystem(None)(f)

  def withTaskSystem[T](c: Config)(f: TaskSystemComponents => T): Option[T] =
    withTaskSystem(Some(c))(f)

  def withTaskSystem[T](s: String)(f: TaskSystemComponents => T): Option[T] =
    withTaskSystem(Some(ConfigFactory.parseString(s)))(f)

  def withTaskSystem[T](
      c: Option[Config]
  )(f: TaskSystemComponents => T): Option[T] = {
    val ts = defaultTaskSystem(c)
    if (ts.hostConfig.myRoles.contains(App)) {
      try {
        Some(f(ts.components))
      } finally {
        ts.shutdown()
      }
    } else None

  }

  def defaultTaskSystem: TaskSystem =
    defaultTaskSystem(None)

  def defaultTaskSystem(string: String): TaskSystem =
    defaultTaskSystem(Some(ConfigFactory.parseString(string)))

  def defaultTaskSystem(extraConf: Option[Config]): TaskSystem = {

    val configuration = () => {
      ConfigFactory.invalidateCaches

      val loaded = (extraConf.map { extraConf =>
        ConfigFactory.defaultOverrides
          .withFallback(extraConf)
          .withFallback(ConfigFactory.load)
      } getOrElse
        ConfigFactory.load)

      ConfigFactory.invalidateCaches

      loaded
    }

    implicit val tconfig = tasks.util.config.parse(configuration)

    val elasticSupport = elastic.makeElasticSupport

    val hostConfig = elasticSupport
      .flatMap(_.hostConfig)
      .getOrElse(MasterSlaveGridEngineChosenFromConfig)

    val finalAkkaConfiguration = {

      val actorProvider = hostConfig match {
        case _: LocalConfiguration => "akka.actor.LocalActorRefProvider"
        case _                     => "akka.remote.RemoteActorRefProvider"
      }

      val akkaProgrammaticalConfiguration = ConfigFactory.parseString(s"""
        task-worker-dispatcher.fork-join-executor.parallelism-max = ${hostConfig.availableCPU}
        task-worker-dispatcher.fork-join-executor.parallelism-min = ${hostConfig.availableCPU}
        
        akka {
          actor {
            provider = "${actorProvider}"
          }
          remote {
            artery {
              canonical.hostname = "${hostConfig.myAddress.getHostName}"
              canonical.port = ${hostConfig.myAddress.getPort.toString}
            }
            
         }
        }
          """)

      ConfigFactory.defaultOverrides
        .withFallback(akkaProgrammaticalConfiguration)
        .withFallback(ConfigFactory.parseResources("akka.conf"))
        .withFallback(configuration())

    }

    val system = ActorSystem(tconfig.actorSystemName, finalAkkaConfiguration)

    new TaskSystem(hostConfig, system, elasticSupport)
  }

  def AsyncTask[A <: AnyRef, C](taskID: String, taskVersion: Int)(
      comp: A => ComputationEnvironment => Future[C]
  ): TaskDefinition[A, C] =
    macro TaskDefinitionMacros
      .taskDefinitionFromTree[A, C]

  def spore[A, B](value: A => B): Spore[A, B] =
    macro tasks.queue.SporeMacros
      .sporeImpl[A, B]

  def spore[B](value: () => B): Spore[Unit, B] =
    macro tasks.queue.SporeMacros
      .sporeImpl0[B]

  implicit def functionToSporeConversion[A, B](value: A => B): Spore[A, B] =
    macro tasks.queue.SporeMacros
      .sporeImpl[A, B]

  implicit def functionToSporeConversion0[B](value: () => B): Spore[Unit, B] =
    macro tasks.queue.SporeMacros
      .sporeImpl0[B]

  def makeSerDe[A]: SerDe[A] = macro SerdeMacro.create[A]

  type SSerializer[T] = Spore[Unit, Serializer[T]]
  type SDeserializer[T] = Spore[Unit, Deserializer[T]]

  implicit def serde2ser[A](a: SerDe[A]): SSerializer[A] = a.ser
  implicit def serde2deser[A](a: SerDe[A]): SDeserializer[A] = a.deser

  def MasterSlaveGridEngineChosenFromConfig(
      implicit config: TasksConfig
  ): HostConfiguration =
    if (config.disableRemoting) new LocalConfigurationFromConfig
    else new MasterSlaveFromConfig

  def appendToFilePrefix[T](
      elements: Seq[String]
  )(implicit ce: ComputationEnvironment): (ComputationEnvironment => T) => T =
    ce.withFilePrefix[T](elements) _

  def fromFileList[I, O](files: Seq[Seq[String]])(
      fromFiles: Seq[SharedFile] => O
  )(full: => Future[O])(implicit tsc: TaskSystemComponents): Future[O] = {
    import tsc.actorsystem.dispatcher
    val filesWithNonEmptyPath = files.filter(_.nonEmpty)
    val logger = akka.event.Logging(tsc.actorsystem, getClass)
    for {
      maybeSharedFiles <- Future
        .sequence(filesWithNonEmptyPath.map { path =>
          val prefix = tsc.filePrefix.append(path.dropRight(1))
          SharedFileHelper
            .getByName(path.last, retrieveSizeAndHash = true)(tsc.fs, prefix)
        })

      validSharedFiles = (maybeSharedFiles zip filesWithNonEmptyPath)
        .map {
          case (None, path) =>
            logger.debug(s"Can't find ${path.mkString("/")}")
            None
          case (valid, _) => valid
        }
        .filter(_.isDefined)
        .map(_.get)

      result <- if (filesWithNonEmptyPath.size == validSharedFiles.size) {
        Future.successful(fromFiles(validSharedFiles))
      } else {
        full
      }

    } yield result

  }

}
