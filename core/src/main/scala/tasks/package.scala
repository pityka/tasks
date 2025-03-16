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

import org.ekrich.config.{Config, ConfigFactory}

import tasks.wire._
import tasks.queue._
import tasks.fileservice._
import tasks.util.config.TasksConfig
import tasks.deploy._
import tasks.shared.LogRecord

import cats.effect.IO
import tasks.shared.ResourceAllocated
import cats.effect.kernel.Resource
import tasks.fileservice.s3.S3Client
import cats.effect.kernel.Deferred
import cats.effect.ExitCode

package object tasks extends MacroCalls {

  val SharedFile = tasks.fileservice.SharedFile

  type SharedFile = tasks.fileservice.SharedFile

  type CodeVersion = tasks.shared.CodeVersion
  val CodeVersion = tasks.shared.CodeVersion

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
      tasks.shared.ResourceRequest(cpu, memory, 0, 0, None)
    )

  def ResourceRequest(cpu: Int, memory: Int)(implicit
      codeVersion: CodeVersion
  ) =
    tasks.shared.VersionedResourceRequest(
      codeVersion,
      tasks.shared.ResourceRequest((cpu, cpu), memory, 0, 0, None)
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
    comp.launcher.release(comp.taskActor)

  implicit def ts(implicit
      component: ComputationEnvironment
  ): TaskSystemComponents =
    component.components

  implicit def launcherActor(implicit
      component: ComputationEnvironment
  ): LauncherActor =
    component.launcher.launcherActor

  implicit def resourceAllocated(implicit
      component: ComputationEnvironment
  ): ResourceAllocated =
    component.resourceAllocated

  def audit(data: String)(implicit component: ComputationEnvironment): Boolean =
    component.appendLog(LogRecord(data, java.time.Instant.now))

  def withTaskSystem[T](
      f: TaskSystemComponents => IO[T]
  ): IO[Either[ExitCode, T]] =
    withTaskSystem(None, Resource.pure(None), Resource.pure(None))(f)

  def withTaskSystem[T](
      config: Config,
      s3Client: Resource[IO, Option[S3Client]],
      elasticSupport: Resource[IO, Option[elastic.ElasticSupport]]
  )(
      f: TaskSystemComponents => IO[T]
  ): IO[Either[ExitCode, T]] =
    withTaskSystem(Some(config), s3Client, elasticSupport)(f)

  def withTaskSystem[T](c: Config)(
      f: TaskSystemComponents => IO[T]
  ): IO[Either[ExitCode, T]] =
    withTaskSystem(Some(c), Resource.pure(None), Resource.pure(None))(f)

  def withTaskSystem[T](config: Option[Config])(
      f: TaskSystemComponents => IO[T]
  ): IO[Either[ExitCode, T]] =
    withTaskSystem(config, Resource.pure(None), Resource.pure(None))(f)

  def withTaskSystem[T](
      config: String,
      s3Client: Resource[IO, Option[S3Client]],
      elasticSupport: Resource[IO, Option[elastic.ElasticSupport]]
  )(
      f: TaskSystemComponents => IO[T]
  ): IO[Either[ExitCode, T]] =
    withTaskSystem(
      Some(ConfigFactory.parseString(config)),
      s3Client,
      elasticSupport
    )(f)

  def withTaskSystem[T](
      config: Option[Config],
      s3Client: Resource[IO, Option[S3Client]],
      elasticSupport: Resource[IO, Option[elastic.ElasticSupport]]
  )(f: TaskSystemComponents => IO[T]): IO[Either[ExitCode, T]] = {

    val resource = Resource.eval(Deferred[IO, ExitCode]).flatMap { exitCode =>
      defaultTaskSystem(config, s3Client, elasticSupport, exitCode).map(tsc =>
        (tsc, exitCode)
      )
    }

    resource.use { case ((tsc, hostConfig), exitCode) =>
      if (hostConfig.myRoles.contains(App)) {

        f(tsc).map(Right(_))
      } else {
        scribe.info(
          "Waiting for exit.."
        )
        // Queue and Worker roles are stopped wthen the exitCode deferred gets filled
        exitCode.get.map(Left(_))

      }
    }

  }

  def defaultTaskSystem
      : Resource[IO, (TaskSystemComponents, HostConfiguration)] =
    Resource.eval(Deferred[IO, ExitCode]).flatMap { exitCode =>
      defaultTaskSystem(
        None,
        Resource.pure(None),
        Resource.pure(None),
        exitCode
      )
    }

  def defaultTaskSystem(
      config: String,
      s3Client: Resource[IO, Option[S3Client]],
      elasticSupport: Resource[IO, Option[elastic.ElasticSupport]]
  ): Resource[IO, (TaskSystemComponents, HostConfiguration)] =
    Resource.eval(Deferred[IO, ExitCode]).flatMap { exitCode =>
      defaultTaskSystem(
        Some(ConfigFactory.parseString(config)),
        s3Client,
        elasticSupport,
        exitCode
      )
    }
  def defaultTaskSystem(
      config: String
  ): Resource[IO, (TaskSystemComponents, HostConfiguration)] =
    Resource.eval(Deferred[IO, ExitCode]).flatMap { exitCode =>
      defaultTaskSystem(
        Some(ConfigFactory.parseString(config)),
        Resource.pure(None),
        Resource.pure(None),
        exitCode
      )
    }

  def defaultTaskSystem(
      config: Option[Config]
  ): Resource[IO, (TaskSystemComponents, HostConfiguration)] =
    Resource.eval(Deferred[IO, ExitCode]).flatMap { exitCode =>
      defaultTaskSystem(
        config,
        Resource.pure(None),
        Resource.pure(None),
        exitCode
      )
    }

  /** The user of this resource should check the role of the returned
    * HostConfiguration If it is not an App, then it is very likely that the
    * correct use case is to return an IO.never
    *
    * @param extraConf
    * @return
    */
  def defaultTaskSystem(
      config: Option[Config],
      s3Client: Resource[IO, Option[S3Client]],
      elasticSupport: Resource[IO, Option[elastic.ElasticSupport]],
      exitCode: Deferred[IO, ExitCode]
  ): Resource[IO, (TaskSystemComponents, HostConfiguration)] = {

    val configuration = () => {
      ConfigFactory.invalidateCaches()

      val loaded = tasks.util.loadConfig(config)

      ConfigFactory.invalidateCaches()

      loaded
    }
    implicit val tconfig = tasks.util.config.parse(configuration)

    val hostConfig = elasticSupport
      .map(
        _.flatMap(_.hostConfig).getOrElse(hostConfigChosenFromConfig)
      )

    TaskSystemComponents.make(
      hostConfig = hostConfig,
      elasticSupport = elasticSupport,
      s3ClientResource = s3Client,
      config = tconfig,
      exitCode = exitCode
    )
  }

  type SSerializer[T] = Spore[Unit, Serializer[T]]
  type SDeserializer[T] = Spore[Unit, Deserializer[T]]

  implicit def serde2ser[A](a: SerDe[A]): SSerializer[A] = a.ser
  implicit def serde2deser[A](a: SerDe[A]): SDeserializer[A] = a.deser

  def hostConfigChosenFromConfig(implicit
      config: TasksConfig
  ): HostConfiguration =
    if (config.disableRemoting) new LocalConfigurationFromConfig
    else new DefaultHostConfigurationFromConfig

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
