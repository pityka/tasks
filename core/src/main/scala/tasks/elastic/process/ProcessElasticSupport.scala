/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
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

package tasks.elastic.process

import tasks.elastic._
import tasks.shared._
import tasks.util.config._
import tasks.util.SimpleSocketAddress
import tasks.util.Uri
import cats.effect.kernel.Ref
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.kernel.Deferred
import cats.effect.ExitCode

final case class ProcessContext(
    name: String,
    hostname: String,
    externalHostname: Option[String],
    resources: ResourceAvailable
) {

  require(name.nonEmpty, "ProcessContext.name must be non-empty")

  require(hostname.nonEmpty, "ProcessContext.hostname must be non-empty")

  def withExternalHostname(value: String): ProcessContext =
    copy(externalHostname = Some(value))

  def withCpu(value: Int): ProcessContext =
    copy(resources = resources.copy(cpu = value))

  def withMemory(value: Int): ProcessContext =
    copy(resources = resources.copy(memory = value))

  def withScratch(value: Int): ProcessContext =
    copy(resources = resources.copy(scratch = value))

  def withGpu(value: List[Int]): ProcessContext =
    copy(resources = resources.copy(gpu = value.distinct.sorted))

  def withLabels(value: Set[String]): ProcessContext =
    copy(resources = resources.copy(labels = value))
}

object ProcessContext {

  def apply(name: String, hostname: String): ProcessContext =
    ProcessContext(
      name = name,
      hostname = hostname,
      externalHostname = None,
      resources = ResourceAvailable(
        cpu = 1,
        memory = 2000,
        scratch = 20000,
        gpu = Nil,
        image = None
      )
    )
}

trait ProcessConfig {

  def contexts: List[ProcessContext]

  def minimumResourceAllocation: Boolean

  def ownsNodeId(nodeId: String): Boolean =
    contexts.exists(context => nodeId.startsWith(context.name + ":"))

}

/** Elastic support for simple process based remote deployments
  *
  * Remote processes are spawned from a list of preconfigured contexts.
  * Interaction with the external world is done by executing processes ('shell
  * out').
  *
  * Self shutdown of workers is always done by exiting the process via an
  * exitcode. The remote spawn and remote shutdown commands are configurable by
  * composing in a RemoteShutdownCommand and SpawnProcessCommand instance.
  */
object ProcessElasticSupport {

  def make(
      processConfig: ProcessConfig,
      shutdownCommand: RemoteShutdownCommand,
      spawnProcessCommand: SpawnProcessCommand
  ): IO[ElasticSupport] = {
    for {
      settings <- ProcessSettings.make(processConfig)
      ref <- Ref.of[IO, Map[RunningJobId, ProcessId]](Map.empty)
    } yield {
      new ElasticSupport(
        hostConfig = None,
        shutdownFromNodeRegistry =
          new ProcessShutdownFromRegistry(ref, shutdownCommand),
        shutdownFromWorker = ShutdownFromWorker,
        createNodeFactory = new ProcessCreateNodeFactory(
          nodeNamesToContainerIds = ref,
          config = processConfig,
          settings = settings,
          spawnProcessCommand = spawnProcessCommand
        ),
        getNodeName = ProcessGetNodeName,
        convertRunningToPending = new ProcessConvertRunningToPending(ref)
      )
    }
  }
}

private object ProcessSettings {

  def make(
      config: ProcessConfig
  ): IO[ProcessSettings] = {

    val hosts = config.contexts
    scribe.info(s"Available contexts: $hosts")

    Ref.of[IO, List[ProcessContext]](hosts).map { r =>
      new ProcessSettings(r, config)
    }
  }

}

private class ProcessSettings(
    hosts: Ref[IO, List[ProcessContext]],
    config: ProcessConfig
) {

  def allocate(h: ResourceRequest) = hosts.modify { hosts =>
    hosts.find(_.resources.canFulfillRequest(h)) match {
      case None => (hosts, None)
      case Some(value) =>
        if (config.minimumResourceAllocation) {
          val list = hosts.filterNot(_.name == value.name) ++ List(
            value.copy(resources = value.resources.substract(h))
          )

          (
            list,
            Option((value.name, value.resources.minimum(h), value))
          )
        } else {
          val list = hosts.filterNot(_.name == value.name) ++ List(
            value.copy(resources = value.resources.substractAll)
          )

          (
            list,
            Option((value.name, value.resources.all, value))
          )
        }
    }
  }

  def deallocate(h: ResourceAllocated, context: String) = hosts.update { list =>
    list.filterNot(_.name == context) ++ List(
      {
        val ho = list.find(_.name == context).get
        ho.copy(resources = ho.resources.addBack(h))
      }
    )
  }

}

private object ShutdownFromWorker extends ShutdownSelfNode {

  def shutdownRunningNode(
      exitCode: Deferred[IO, ExitCode],
      nodeName: RunningJobId
  ): IO[Unit] = {
    IO {
      scribe.info("Shut down current process with System.exit", nodeName)
    } *> exitCode.complete(ExitCode(0)).map((_: Boolean) => ())

  }

  def shutdownPendingNode(nodeName: PendingJobId): IO[Unit] = IO.unit

}
trait RemoteShutdownCommand {
  def apply(contextName: String, processId: ProcessId): List[String]
}
private class ProcessShutdownFromRegistry(
    nodeNamesToContainerIds: Ref[IO, Map[RunningJobId, ProcessId]],
    shutdownCommand: RemoteShutdownCommand
) extends ShutdownNode {

  def shutdownRunningNode(nodeName: RunningJobId): IO[Unit] = {
    val contextName = nodeName.value.split(":")(0)
    val n = nodeName.value.split(":")(1)
    nodeNamesToContainerIds.get
      .map(
        _.get(nodeName)
      )
      .flatMap {
        case None =>
          IO(
            scribe.error(
              s"Process id not found for runnig job id can't shut it down",
              nodeName
            )
          )
        case Some(processId) =>
          import scala.sys.process._
          IO.interruptible(
            shutdownCommand(contextName = contextName, processId = processId).!
          )

      }

  }

  def shutdownPendingNode(nodeName: PendingJobId): IO[Unit] = {
    val contextName = nodeName.value.split(":")(0)
    val processId = ProcessId(nodeName.value.split(":")(1))
    import scala.sys.process._
    IO.interruptible(
      shutdownCommand(contextName = contextName, processId = processId).!
    )
  }

}

// private final case class NodeName(s: String)
case class ProcessId(s: String)

trait SpawnProcessCommand {

  /** Returns an external command with its parameters which when executed prints
    * to the standard output a string which serves as the process id
    */
  def apply(
      context: String,
      allocated: ResourceAllocated,
      script: String
  ): List[String]

  /** If true command script will spawn in the background with nohup returning a
    * pid on stdout
    *
    * @return
    */
  def background: Boolean
}

private final class ProcessConvertRunningToPending(
    nodeNamesToContainerIds: Ref[IO, Map[RunningJobId, ProcessId]]
) extends ConvertRunningToPending {
  override def convertRunningToPending(
      p: RunningJobId
  ): IO[Option[PendingJobId]] = {
    val spl = p.value.split(":")
    val contextName = spl(0)
    val nodeName = p
    nodeNamesToContainerIds.get
      .map(map =>
        map
          .get(nodeName)
          .map(processId =>
            PendingJobId(s"$contextName:${processId.s}")
          ) match {
          case None =>
            scribe.error(s"Found no nodename in $map", nodeName)
            None
          case Some(value) =>
            Some(value)
        }
      )

  }
}

private final class ProcessCreateNode(
    masterAddress: SimpleSocketAddress,
    masterPrefix: String,
    codeAddress: CodeAddress,
    nodeNamesToContainerIds: Ref[IO, Map[RunningJobId, ProcessId]],
    config: ProcessConfig,
    settings: ProcessSettings,
    spawnProcessCommand: SpawnProcessCommand
) extends CreateNode {

  private val converter =
    new ProcessConvertRunningToPending(nodeNamesToContainerIds)

  override def convertRunningToPending(
      p: RunningJobId
  ): IO[Option[PendingJobId]] = converter.convertRunningToPending(p)

  def requestOneNewJobFromJobScheduler(
      requestSize: ResourceRequest
  )(implicit
      tasksConfig: TasksConfig
  ): IO[Either[String, (PendingJobId, ResourceAvailable)]] =
    settings
      .allocate(requestSize)
      .flatMap {
        case None => IO.raiseError(new RuntimeException("can't fulfill"))
        case Some((context, allocated, host)) =>
          val nodeName = RunningJobId(
            s"$context:${scala.util.Random.alphanumeric.take(32).mkString}"
          )
          val script = Deployment.script(
            memory = allocated.memory,
            cpu = allocated.cpu,
            scratch = allocated.scratch,
            gpus = allocated.gpu.zipWithIndex.map(_._2),
            masterAddress = masterAddress,
            masterPrefix = masterPrefix,
            download = Uri(
              scheme = "http",
              hostname = codeAddress.address.getHostName,
              port = codeAddress.address.getPort,
              path = "/"
            ),
            followerHostname = Some(host.hostname),
            followerExternalHostname =
              Some(host.externalHostname.getOrElse(host.hostname)),
            followerMayUseArbitraryPort = true,
            followerNodeName = Some(nodeName.value),
            background = spawnProcessCommand.background,
            image = requestSize.image
          )

          val cmd: List[String] =
            spawnProcessCommand(context, allocated, script)

          scribe.debug(
            s"Exec",
            scribe.data("exec-list", cmd.mkString("['", "','", "']")),
            scribe.data("explain", "command to spawn new process")
          )
          fs2.io.process.ProcessBuilder
            .apply(cmd.head, cmd.drop(1))
            .spawn[IO]
            .flatMap { process =>
              val stdout = process.stdout
                .through(fs2.text.utf8.decode)
                .compile
                .fold("")(_ + _)
              val stderr = process.stderr
                .through(fs2.text.utf8.decode)
                .compile
                .fold("")(_ + _)
              val exitcode = process.exitValue.timeout(
                scala.concurrent.duration.Duration
                  .apply(60, scala.concurrent.duration.SECONDS)
              )

              Resource.eval(IO.both(stdout, stderr).both(exitcode).map {
                case ((stdout, stderr), exitcode) =>
                  if (exitcode != 0) {
                    scribe.error(
                      s"Failed to spawn process",
                      scribe.data(
                        Map(
                          "exit-code" -> exitcode,
                          "stderr" -> stderr,
                          "stdout" -> stdout
                        )
                      )
                    )
                    ???
                  } else stdout.trim
              })

            }
            .use { processId =>
              scribe.info(
                s"ProcessId",
                scribe.data("process-id", processId),
                scribe.data("explain", "PID of spawned process"),
                nodeName,
                requestSize
              )
              nodeNamesToContainerIds
                .update { map =>
                  map.updated(nodeName, ProcessId(processId))
                }
                .map(_ =>
                  (
                    PendingJobId(context + ":" + processId),
                    ResourceAvailable(
                      cpu = allocated.cpu,
                      memory = allocated.memory,
                      scratch = allocated.scratch,
                      gpu = allocated.gpu,
                      image = requestSize.image
                    )
                  )
                )

            }
      }
      .attempt
      .map { either =>
        either.left.map { e =>
          scribe.debug("Failed node request", e)
          e.getMessage
        }
      }

}

private final class ProcessCreateNodeFactory(
    nodeNamesToContainerIds: Ref[IO, Map[RunningJobId, ProcessId]],
    config: ProcessConfig,
    settings: ProcessSettings,
    spawnProcessCommand: SpawnProcessCommand
) extends CreateNodeFactory {
  def apply(
      master: SimpleSocketAddress,
      masterPrefix: String,
      codeAddress: CodeAddress
  ) =
    new ProcessCreateNode(
      masterAddress = master,
      masterPrefix = masterPrefix,
      codeAddress = codeAddress,
      nodeNamesToContainerIds = nodeNamesToContainerIds,
      config = config,
      settings = settings,
      spawnProcessCommand = spawnProcessCommand
    )
}

private object ProcessGetNodeName extends GetNodeName {
  def getNodeName(config: TasksConfig) = IO.pure {
    RunningJobId(config.nodeName)
  }
}
