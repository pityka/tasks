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

package tasks.elastic.docker

import scala.util._

import scala.jdk.CollectionConverters._
import java.io.File
import com.typesafe.config.{Config, ConfigObject}

import tasks.elastic._
import tasks.shared._
import tasks.util.config._
import tasks.util.SimpleSocketAddress
import tasks.util.Uri
import cats.effect.kernel.Ref
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.effect.kernel.Resource
import com.typesafe.config.ConfigFactory

/** Elastic support for simple docker context based remote deployments
  *
  * Remote containers are spawned from a list of preconfigured docker contexts.
  * The application must run on a host which can execute docker --context .. run
  * .. The network of the containers are set to --network=host so that the
  * docker networking is skipped. This is needed because the application is not
  * part of docker networking.
  *
  * The feature where workers shutting down themselves due to lost connection to
  * the node registry is not working.
  */
object DockerElasticSupport {

  def make(config: Option[Config]): Resource[IO, ElasticSupport] = {
    val dockerConfig = new DockerConfig(tasks.util.loadConfig(config))
    Resource.eval(Ref.of[IO, Map[NodeName, ContainerId]](Map.empty)).flatMap {
      ref =>
        cats.effect.Resource.pure(
          SimpleElasticSupport(
            hostConfig = None,
            shutdown = new DockerShutdown(ref),
            createNodeFactory = new DockerCreateNodeFactory(ref, dockerConfig),
            getNodeName = DockerGetNodeName
          )
        )
    }
  }
}

class DockerConfig(raw: Config) {
  val dockerImageName = raw.getString("tasks.docker.image")

  val dockerEnvVars =
    raw.getStringList("tasks.docker.env").asScala.grouped(2).map { g =>
      (g(0), g(1))
    }
  def dockerContexts = raw.getConfigList("tasks.docker.contexts").asScala
  val dockerNetwork = raw.getString("tasks.docker.network")
}

object DockerSettings {
  case class Host(
      context: String,
      resourceAvailable: ResourceAvailable,
      hostname: String,
      externalHostname: Option[String]
  )
  object Host {
    def fromConfig(config: Config) = {
      val context = config.getString("context")
      val memory = Try(config.getInt("memory")).toOption.getOrElse(Int.MaxValue)
      val cpu = Try(config.getInt("cpu")).toOption.getOrElse(1)
      val scratch =
        Try(config.getInt("scratch")).toOption.getOrElse(Int.MaxValue)
      val gpu =
        Try(config.getIntList("gpu").asScala.map(_.toInt).toList).toOption
          .getOrElse(Nil)
      Host(
        context,
        ResourceAvailable(cpu, memory, scratch, gpu, image = None),
        config.getString("hostname"),
        Try(config.getString("externalHostname")).toOption
      )
    }
  }

  implicit def fromConfig(implicit config: DockerConfig): IO[DockerSettings] = {
    val hosts =
      config.dockerContexts.map { case config =>
        Host.fromConfig(config)

      }.toList
    Ref.of[IO, List[Host]](hosts).map(r => new DockerSettings(r, config))
  }

}

class DockerSettings(
    hosts: Ref[IO, List[DockerSettings.Host]],
    config: DockerConfig
) {
  import DockerSettings._

  def allocate(h: ResourceRequest) = hosts.modify { hosts =>
    hosts.find(_.resourceAvailable.canFulfillRequest(h)) match {
      case None => (hosts, None)
      case Some(value) =>
        val list = hosts.filterNot(_.context == value.context) ++ List(
          value.copy(resourceAvailable = value.resourceAvailable.substract(h))
        )

        (
          list,
          Option((value.context, value.resourceAvailable.minimum(h), value))
        )
    }
  }

  def deallocate(h: ResourceAllocated, context: String) = hosts.update { list =>
    list.filterNot(_.context == context) ++ List(
      {
        val ho = list.find(_.context == context).get
        ho.copy(resourceAvailable = ho.resourceAvailable.addBack(h))
      }
    )
  }

}

class DockerShutdown(
    nodeNamesToContainerIds: Ref[IO, Map[NodeName, ContainerId]]
) extends ShutdownNode {

  def shutdownRunningNode(nodeName: RunningJobId): Unit = {
    val contextName = nodeName.value.split(":")(0)
    val n = nodeName.value.split(":")(1)
    val containerId = nodeNamesToContainerIds.get
      .map(
        _.get(NodeName(n))
      )
      .unsafeRunSync()
      .map(_.s) match {
      case None =>
        scribe.error(
          s"Container id not found for runnig job id $nodeName can't shut it down"
        )
      case Some(containerId) =>
        s"docker --context $contextName kill $containerId"
    }

  }

  def shutdownPendingNode(nodeName: PendingJobId): Unit = ()

}

case class NodeName(s: String)
case class ContainerId(s: String)

class DockerCreateNode(
    masterAddress: SimpleSocketAddress,
    codeAddress: CodeAddress,
    nodeNamesToContainerIds: Ref[IO, Map[NodeName, ContainerId]],
    config: DockerConfig
) extends CreateNode {

  val settings = DockerSettings.fromConfig(config).unsafeRunSync()

  override def convertRunningToPending(
      p: RunningJobId
  ): Option[PendingJobId] = {
    val spl = p.value.split(":")
    val contextName = spl(0)
    val nodeName = NodeName(spl(1))
    nodeNamesToContainerIds.get
      .map(
        _.get(nodeName).map(containerId =>
          PendingJobId(s"$contextName:${containerId.s}")
        )
      )
      .unsafeRunSync()
  }

  def requestOneNewJobFromJobScheduler(
      requestSize: ResourceRequest
  )(implicit tasksConfig: TasksConfig): Try[(PendingJobId, ResourceAvailable)] =
    settings
      .allocate(requestSize)
      .flatMap {
        case None => IO.raiseError(new RuntimeException("can't fulfill"))
        case Some((context, allocated, host)) =>
          val nodeName = scala.util.Random.alphanumeric.take(32).mkString
          val script = Deployment.script(
            memory = allocated.memory,
            cpu = allocated.cpu,
            scratch = allocated.scratch,
            gpus = allocated.gpu.zipWithIndex.map(_._2),
            masterAddress = masterAddress,
            download = Uri(
              scheme = "http",
              hostname = codeAddress.address.getHostName,
              port = codeAddress.address.getPort,
              path = "/"
            ),
            followerHostname = Some(host.hostname),
            followerExternalHostname =
              Some(host.externalHostname.getOrElse(host.hostname)),
            followerMayUseArbitraryPort = false,
            followerNodeName = Some(nodeName),
            background = false,
            image = requestSize.image
          )

          val cmd = List(
            "docker",
            "run",
            "-d",
            s"--network=${config.dockerNetwork}"
          ) ++ config.dockerEnvVars.toList.flatMap { case (k, v) =>
            List("--env", s"$k=$v")
          } ++ (if (allocated.gpu.nonEmpty)
                  List("--gpus", allocated.gpu.mkString(","))
                else Nil) ++
            List(
              requestSize.image
                .orElse(Option(config.dockerImageName).filter(_.nonEmpty))
                .getOrElse("eclipse-temurin:17.0.13_11-jre-ubi9-minimal"),
              "/bin/bash",
              "-c",
              script
            )
          scribe.info(s"Exec $cmd")
          IO(
            scala.sys.process.Process
              .apply(
                cmd
              )
              .!!
          ).flatMap { containerId =>
            nodeNamesToContainerIds
              .update { map =>
                map.updated(NodeName(nodeName), ContainerId(containerId))
              }
              .map(_ =>
                (
                  PendingJobId(context + ":" + containerId),
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
      .map { _.toTry }
      .unsafeRunSync()

}

class DockerCreateNodeFactory(
    nodeNamesToContainerIds: Ref[IO, Map[NodeName, ContainerId]],
    config: DockerConfig
) extends CreateNodeFactory {
  def apply(master: SimpleSocketAddress, codeAddress: CodeAddress) =
    new DockerCreateNode(master, codeAddress, nodeNamesToContainerIds, config)
}

object DockerGetNodeName extends GetNodeName {
  def getNodeName = {
    System.getProperty("tasks.elastic.nodename")
  }
}
