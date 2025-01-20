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

import com.typesafe.config.{Config, ConfigFactory}

import tasks.wire._
import tasks.queue._
import tasks.fileservice._
import tasks.util.config.TasksConfig
import tasks.deploy._
import tasks.shared.LogRecord

import cats.effect.IO
import tasks.shared.ResourceAllocated
import cats.effect.kernel.Resource

package object tasks extends MacroCalls {

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
      tasks.shared.ResourceRequest(cpu, memory, scratch, gpu, None)
    )
  def ResourceRequest(cpu: (Int, Int), memory: Int, scratch: Int)(implicit
      codeVersion: CodeVersion
  ) =
    tasks.shared.VersionedResourceRequest(
      codeVersion,
      tasks.shared.ResourceRequest(cpu, memory, scratch, 0, None)
    )

  def ResourceRequest(cpu: (Int, Int), memory: Int)(implicit
      codeVersion: CodeVersion
  ) =
    tasks.shared.VersionedResourceRequest(
      codeVersion,
      tasks.shared.ResourceRequest(cpu, memory, 1, 0, None)
    )

  def ResourceRequest(cpu: Int, memory: Int)(implicit
      codeVersion: CodeVersion
  ) =
    tasks.shared.VersionedResourceRequest(
      codeVersion,
      tasks.shared.ResourceRequest((cpu, cpu), memory, 1, 0, None)
    )

  def ResourceRequest(cpu: Int, memory: Int, scratch: Int)(implicit
      codeVersion: CodeVersion
  ) =
    tasks.shared.VersionedResourceRequest(
      codeVersion,
      tasks.shared.ResourceRequest(cpu, memory, scratch, 0)
    )

  implicit def tasksConfig(implicit
      component: TaskSystemComponents
  ): TasksConfig =
    component.tasksConfig

  implicit def codeVersionFromTasksConfig(implicit
      c: TasksConfig
  ): CodeVersion = c.codeVersion

  implicit def fs(implicit
      component: TaskSystemComponents
  ): FileServiceComponent =
    component.fs

  def releaseResourcesEarly(implicit comp: ComputationEnvironment) =
    IO.delay(comp.launcher.actor ! Launcher.Release(comp.taskActor))

  implicit def ts(implicit
      component: ComputationEnvironment
  ): TaskSystemComponents =
    component.components

  implicit def launcherActor(implicit
      component: ComputationEnvironment
  ): LauncherActor =
    component.launcher

  implicit def resourceAllocated(implicit
      component: ComputationEnvironment
  ): ResourceAllocated =
    component.resourceAllocated

  def audit(data: String)(implicit component: ComputationEnvironment): Boolean =
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
    import cats.effect.unsafe.implicits.global

    val resource = defaultTaskSystem(c)
    val ((tsc, hostConfig), shutdown) = resource.allocated.unsafeRunSync()
    if (hostConfig.myRoles.contains(App)) {
      try {
        Some(f(tsc))
      } finally {
        shutdown.unsafeRunSync()
      }
    } else {
      scribe.info(
        "Leaving withTaskSystem lambda without closing taskystem. This is only meaningful for forever running worker node."
      )
      // Queue and Worker roles are never stopped
      None
    }

  }

  def defaultTaskSystem
      : Resource[IO, (TaskSystemComponents, HostConfiguration)] =
    defaultTaskSystem(None)

  def defaultTaskSystem(
      string: String
  ): Resource[IO, (TaskSystemComponents, HostConfiguration)] =
    defaultTaskSystem(Some(ConfigFactory.parseString(string)))

  /** The user of this resource should check the role of the returned
    * HostConfiguration If it is not an App, then it is very likely that the
    * correct use case is to return an IO.never
    *
    * @param extraConf
    * @return
    */
  def defaultTaskSystem(
      extraConf: Option[Config]
  ): Resource[IO, (TaskSystemComponents, HostConfiguration)] = {

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
      .map(
        _.flatMap(_.hostConfig).getOrElse(MasterSlaveGridEngineChosenFromConfig)
      )

    TaskSystemComponents.make(hostConfig, elasticSupport, tconfig)
  }

  type SSerializer[T] = Spore[Unit, Serializer[T]]
  type SDeserializer[T] = Spore[Unit, Deserializer[T]]

  implicit def serde2ser[A](a: SerDe[A]): SSerializer[A] = a.ser
  implicit def serde2deser[A](a: SerDe[A]): SDeserializer[A] = a.deser

  def MasterSlaveGridEngineChosenFromConfig(implicit
      config: TasksConfig
  ): HostConfiguration =
    if (config.disableRemoting) new LocalConfigurationFromConfig
    else new MasterSlaveFromConfig

  def appendToFilePrefix[T](
      elements: Seq[String]
  )(implicit ce: ComputationEnvironment): (ComputationEnvironment => T) => T =
    ce.withFilePrefix[T](elements) _

  def fromFileList[O](files: Seq[Seq[String]], parallelism: Int)(
      fromFiles: Seq[SharedFile] => O
  )(full: => IO[O])(implicit tsc: TaskSystemComponents): IO[O] = {
    val filesWithNonEmptyPath = files.filter(_.nonEmpty)
    for {
      maybeSharedFiles <- IO.parSequenceN(parallelism)(
        filesWithNonEmptyPath.map { path =>
          val prefix = tsc.filePrefix.append(path.dropRight(1))
          SharedFileHelper
            .getByName(path.last, retrieveSizeAndHash = true)(tsc.fs, prefix)
        }
      )

      validSharedFiles = (maybeSharedFiles zip filesWithNonEmptyPath)
        .map {
          case (None, path) =>
            scribe.debug(s"Can't find ${path.mkString("/")}")
            None
          case (valid, _) => valid
        }
        .filter(_.isDefined)
        .map(_.get)

      result <-
        if (filesWithNonEmptyPath.size == validSharedFiles.size) {
          IO.pure(fromFiles(validSharedFiles))
        } else {
          full
        }

    } yield result

  }

}
