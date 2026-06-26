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
import tasks.util.Transaction
import tasks.util.message.LauncherName

/** Persistent memoization of asynchronous, distributed computations.
  *
  * ==Overview==
  *
  * A `Task[A, B]` is a named, versioned function `A => IO[B]` whose result is
  * cached on durable storage. Submitting a task with the same input twice runs
  * the body once: the second submission returns the previously stored result
  * without re-executing. The cache survives process restarts, and is shared
  * across all workers in a deployment, so memoization works across machines as
  * well as across runs.
  *
  * The runtime is structured around three cooperating pieces:
  *
  *   - a [[tasks.queue.QueueImpl Queue]] that holds submitted tasks and
  *     schedules them onto workers according to their declared
  *     [[ResourceRequest]] (CPU, memory, scratch, GPU);
  *   - a [[tasks.fileservice.FileServiceComponent FileService]] that stores
  *     task outputs and large blobs in content-addressed storage (local
  *     filesystem or S3), exposed to user code as opaque
  *     [[tasks.fileservice.SharedFile SharedFile]] references;
  *   - a [[tasks.caching.TaskResultCache TaskResultCache]] that maps a task's
  *     identity to its stored result.
  *
  * Build and tear down the whole stack with [[withTaskSystem]] or
  * [[defaultTaskSystem]]; configuration is HOCON, with defaults in
  * `reference.conf`.
  *
  * ==How persistent caching works==
  *
  * Every cached entry is keyed by a
  * [[tasks.queue.HashedTaskDescription HashedTaskDescription]], which combines:
  *
  *   1. the task's [[tasks.queue.TaskId TaskId]] — the user-chosen name plus a
  *      monotonically increasing integer version. Bumping the version is the
  *      explicit, intentional way to invalidate previously cached results when
  *      the task body changes in a way that should produce different output;
  *   1. a hash of the task's serialized input. Inputs are serialized via the
  *      task's `JsonValueCodec` (jsoniter-scala by default; circe and uPickle
  *      are also supported), so two inputs that serialize to the same bytes are
  *      treated as the same key.
  *
  * In addition, every `ResourceRequest` carries an implicit
  * [[tasks.shared.CodeVersion CodeVersion]] derived from the build, which lets
  * deployments distinguish results produced by different binary versions of the
  * worker code when desired.
  *
  * On submission, the queue first asks the cache for an entry under that key.
  *
  *   - On a hit, the stored result (which may include
  *     [[tasks.fileservice.SharedFile SharedFile]] references pointing into the
  *     file service) is returned directly to the caller. No worker is
  *     allocated, no resources are consumed.
  *   - On a miss, the task is scheduled to a worker. When the worker finishes,
  *     its return value is serialized, any files it produced are uploaded into
  *     content-addressed storage as `SharedFile`s, and the cache entry is
  *     written before the result is handed back to the caller. Subsequent
  *     submissions with the same key short-circuit through the cache path.
  *
  * The cache itself is pluggable (see [[tasks.caching.Cache]]); the file
  * service backend is pluggable too (local FS, S3). For high-availability
  * deployments the in-memory queue can be swapped for the durable PostgreSQL
  * queue in the `postgres` module.
  *
  * ==Files as first-class cached values==
  *
  * Task outputs frequently include large files. Rather than embedding bytes in
  * the result, user code returns [[tasks.fileservice.SharedFile SharedFile]]
  * handles. A `SharedFile` is an opaque, content-addressed reference: the bytes
  * live in the file service, and the handle is what gets serialized into the
  * cache. This means a cache hit returns the handle immediately, and the actual
  * bytes are only fetched if and when downstream code reads them — and remain
  * available to any worker, on any machine, that asks for them.
  *
  * ==Putting it together==
  *
  * The expected flow for a user of this library:
  *
  *   1. Define a task with a stable name, a version, and a `JsonValueCodec` for
  *      its input and output.
  *   1. Stand up a task system with [[withTaskSystem]].
  *   1. Submit the task with a [[ResourceRequest]]; receive an `IO` that yields
  *      the (possibly cached) result.
  *   1. When the task body changes in a way that should invalidate stored
  *      results, bump its version.
  */
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

  /** Build a ResourceRequest with an explicit [[tasks.shared.NodeSelector]] for
    * node affinity / avoidance.
    */
  def ResourceRequest(
      cpu: (Int, Int),
      memory: Int,
      scratch: Int,
      gpu: Int,
      nodeSelector: tasks.shared.NodeSelector
  )(implicit codeVersion: CodeVersion) =
    tasks.shared.VersionedResourceRequest(
      codeVersion,
      tasks.shared
        .ResourceRequest(cpu, memory, scratch, gpu, None, Some(nodeSelector))
    )

  def ResourceRequest(
      cpu: Int,
      memory: Int,
      nodeSelector: tasks.shared.NodeSelector
  )(implicit codeVersion: CodeVersion) =
    tasks.shared.VersionedResourceRequest(
      codeVersion,
      tasks.shared.ResourceRequest(
        (cpu, cpu),
        memory,
        0,
        0,
        None,
        Some(nodeSelector)
      )
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

  implicit def ts(implicit
      component: ComputationEnvironment
  ): TaskSystemComponents =
    component.components

  implicit def launcherName(implicit
      component: ComputationEnvironment
  ): LauncherName =
    component.launcher.address

  implicit def resourceAllocated(implicit
      component: LeafComputationEnvironment
  ): ResourceAllocated =
    component.resourceAllocated

  def audit(data: String)(implicit component: ComputationEnvironment): Boolean =
    component.appendLog(LogRecord(data, java.time.Instant.now))

  def withTaskSystem[T](
      f: TaskSystemComponents => IO[T]
  ): IO[Either[ExitCode, T]] =
    withTaskSystem(
      config = None,
      s3Client = Resource.pure(None),
      elasticSupport = Resource.pure(None),
      externalQueueState = Resource.pure(None)
    )(f)

  def withTaskSystem[T](
      config: Config,
      s3Client: Resource[IO, Option[S3Client]],
      elasticSupport: Resource[IO, Option[elastic.ElasticSupport]]
  )(
      f: TaskSystemComponents => IO[T]
  ): IO[Either[ExitCode, T]] =
    withTaskSystem(
      config = Some(config),
      s3Client = s3Client,
      elasticSupport = elasticSupport,
      externalQueueState = Resource.pure(None)
    )(f)

  def withTaskSystem[T](c: Config)(
      f: TaskSystemComponents => IO[T]
  ): IO[Either[ExitCode, T]] =
    withTaskSystem(
      config = Some(c),
      s3Client = Resource.pure(None),
      elasticSupport = Resource.pure(None),
      externalQueueState = Resource.pure(None)
    )(f)

  def withTaskSystem[T](config: Option[Config])(
      f: TaskSystemComponents => IO[T]
  ): IO[Either[ExitCode, T]] =
    withTaskSystem(
      config = config,
      s3Client = Resource.pure(None),
      elasticSupport = Resource.pure(None),
      externalQueueState = Resource.pure(None)
    )(f)

  def withTaskSystem[T](
      config: String,
      s3Client: Resource[IO, Option[S3Client]],
      elasticSupport: Resource[IO, Option[elastic.ElasticSupport]],
      externalQueueState: Resource[IO, Option[Transaction[QueueImpl.State]]]
  )(
      f: TaskSystemComponents => IO[T]
  ): IO[Either[ExitCode, T]] =
    withTaskSystem(
      config = Some(ConfigFactory.parseString(config)),
      s3Client = s3Client,
      elasticSupport = elasticSupport,
      externalQueueState = externalQueueState
    )(f)

  def withTaskSystem[T](
      config: Option[Config],
      s3Client: Resource[IO, Option[S3Client]],
      elasticSupport: Resource[IO, Option[elastic.ElasticSupport]],
      externalQueueState: Resource[IO, Option[Transaction[QueueImpl.State]]],
      meterProvider: Resource[
        IO,
        org.typelevel.otel4s.metrics.MeterProvider[IO]
      ] = Resource.pure[IO, org.typelevel.otel4s.metrics.MeterProvider[IO]](
        org.typelevel.otel4s.metrics.MeterProvider.noop[IO]
      )
  )(f: TaskSystemComponents => IO[T]): IO[Either[ExitCode, T]] = {

    val resource = Resource.eval(Deferred[IO, ExitCode]).flatMap { exitCode =>
      defaultTaskSystem(
        config = config,
        s3Client = s3Client,
        elasticSupport = elasticSupport,
        externalQueueState = externalQueueState,
        exitCode = exitCode,
        meterProvider = meterProvider
      ).map(tsc => (tsc, exitCode))
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
        config = None,
        s3Client = Resource.pure(None),
        elasticSupport = Resource.pure(None),
        externalQueueState = Resource.pure(None),
        exitCode = exitCode
      )
    }

  def defaultTaskSystem(
      config: String,
      s3Client: Resource[IO, Option[S3Client]],
      elasticSupport: Resource[IO, Option[elastic.ElasticSupport]]
  ): Resource[IO, (TaskSystemComponents, HostConfiguration)] =
    Resource.eval(Deferred[IO, ExitCode]).flatMap { exitCode =>
      defaultTaskSystem(
        config = Some(ConfigFactory.parseString(config)),
        s3Client = s3Client,
        elasticSupport = elasticSupport,
        externalQueueState = Resource.pure(None),
        exitCode = exitCode
      )
    }
  def defaultTaskSystem(
      config: String,
      s3Client: Resource[IO, Option[S3Client]],
      elasticSupport: Resource[IO, Option[elastic.ElasticSupport]],
      externalQueueState: Resource[IO, Option[Transaction[QueueImpl.State]]]
  ): Resource[IO, (TaskSystemComponents, HostConfiguration)] =
    Resource.eval(Deferred[IO, ExitCode]).flatMap { exitCode =>
      defaultTaskSystem(
        config = Some(ConfigFactory.parseString(config)),
        s3Client = s3Client,
        elasticSupport = elasticSupport,
        externalQueueState = externalQueueState,
        exitCode = exitCode
      )
    }
  def defaultTaskSystem(
      config: String
  ): Resource[IO, (TaskSystemComponents, HostConfiguration)] =
    Resource.eval(Deferred[IO, ExitCode]).flatMap { exitCode =>
      defaultTaskSystem(
        config = Some(ConfigFactory.parseString(config)),
        s3Client = Resource.pure(None),
        elasticSupport = Resource.pure(None),
        externalQueueState = Resource.pure(None),
        exitCode = exitCode
      )
    }

  def defaultTaskSystem(
      config: Option[Config]
  ): Resource[IO, (TaskSystemComponents, HostConfiguration)] =
    Resource.eval(Deferred[IO, ExitCode]).flatMap { exitCode =>
      defaultTaskSystem(
        config = config,
        s3Client = Resource.pure(None),
        elasticSupport = Resource.pure(None),
        externalQueueState = Resource.pure(None),
        exitCode = exitCode
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
      externalQueueState: Resource[IO, Option[Transaction[QueueImpl.State]]],
      exitCode: Deferred[IO, ExitCode],
      meterProvider: Resource[
        IO,
        org.typelevel.otel4s.metrics.MeterProvider[IO]
      ] = Resource.pure[IO, org.typelevel.otel4s.metrics.MeterProvider[IO]](
        org.typelevel.otel4s.metrics.MeterProvider.noop[IO]
      )
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
      externalQueueState = externalQueueState,
      config = tconfig,
      exitCode = exitCode,
      meterProviderResource = meterProvider
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
