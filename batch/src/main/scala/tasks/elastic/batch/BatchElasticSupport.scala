/*
 * The MIT License
 *
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

package tasks.elastic.batch

import scala.util._

import tasks.elastic._
import tasks.shared._
import tasks.util._
import tasks.util.config._
import tasks.deploy._

import software.amazon.awssdk.services.batch.BatchClient
import software.amazon.awssdk.services.batch.model._
import software.amazon.awssdk.regions.Region

import scala.jdk.CollectionConverters._
import org.ekrich.config.Config
import cats.effect.IO
import cats.effect.kernel.Deferred
import cats.effect.ExitCode
import tasks.util.message.Node

class BatchShutdown(batch: BatchClient) extends ShutdownNode with ShutdownSelfNode {

  def shutdownRunningNode(nodeName: RunningJobId): IO[Unit] =
    IO.interruptible {
      batch.terminateJob(
        TerminateJobRequest.builder
          .jobId(nodeName.value)
          .reason("Shut down by tasks framework")
          .build
      )
      ()
    }

  def shutdownRunningNode(
      exitCode: Deferred[IO, ExitCode],
      nodeName: RunningJobId
  ): IO[Unit] =
    IO.interruptible {
      batch.terminateJob(
        TerminateJobRequest.builder
          .jobId(nodeName.value)
          .reason("Shut down by tasks framework")
          .build
      )
      ()
    }

  def shutdownPendingNode(nodeName: PendingJobId): IO[Unit] =
    IO.interruptible {
      batch.cancelJob(
        CancelJobRequest.builder
          .jobId(nodeName.value)
          .reason("Cancelled by tasks framework")
          .build
      )
      ()
    }
}

class BatchCreateNode(
    masterAddress: SimpleSocketAddress,
    masterPrefix: String,
    codeAddress: CodeAddress,
    batch: BatchClient,
    batchConfig: BatchConfig
) extends CreateNode {

  def requestOneNewJobFromJobScheduler(
      requestSize: ResourceRequest
  )(implicit
      config: TasksConfig
  ): IO[Either[String, (PendingJobId, ResourceAvailable)]] =
    IO.interruptible {
      Try {
        val selectedResources = selectResources(requestSize)

        val script = Deployment.script(
          memory = selectedResources.memory,
          cpu = selectedResources.cpu,
          scratch = selectedResources.scratch,
          gpus = selectedResources.gpu,
          masterAddress = masterAddress,
          masterPrefix = masterPrefix,
          download = Uri(
            scheme = "http",
            hostname = codeAddress.address.getHostName,
            port = codeAddress.address.getPort,
            path = "/"
          ),
          followerHostname = None,
          followerExternalHostname = None,
          followerNodeName = None,
          followerMayUseArbitraryPort = true,
          background = false,
          image = None
        )(config)

        val resourceReqs = {
          val reqs = List(
            ResourceRequirement.builder
              .`type`(ResourceType.VCPU)
              .value(selectedResources.cpu.toString)
              .build,
            ResourceRequirement.builder
              .`type`(ResourceType.MEMORY)
              .value(selectedResources.memory.toString)
              .build
          ) ++ (if (selectedResources.gpu.nonEmpty)
                  List(
                    ResourceRequirement.builder
                      .`type`(ResourceType.GPU)
                      .value(selectedResources.gpu.size.toString)
                      .build
                  )
                else Nil)
          reqs.asJava
        }

        val containerOverrides = ContainerOverrides.builder
          .resourceRequirements(resourceReqs)
          .command("/bin/bash", "-c", script)
          .build

        val submitRequest = SubmitJobRequest.builder
          .jobName(
            "tasks-worker-" + java.util.UUID.randomUUID.toString.take(8)
          )
          .jobQueue(batchConfig.jobQueue)
          .jobDefinition(batchConfig.jobDefinition)
          .containerOverrides(containerOverrides)
          .tags(batchConfig.tags.asJava)
          .build

        val result = batch.submitJob(submitRequest)
        val jobId = PendingJobId(result.jobId)
        (jobId, selectedResources)
      }.toEither.left.map(_.getMessage)
    }

  override def convertRunningToPending(
      p: RunningJobId
  ): IO[Option[PendingJobId]] =
    IO.pure(Some(PendingJobId(p.value)))

  private def selectResources(
      requestSize: ResourceRequest
  ): ResourceAvailable = {
    val cpu = math.max(requestSize.cpu._1, batchConfig.minimumCpu)
    val memory = math.max(requestSize.memory, batchConfig.minimumMemory)
    val scratch = requestSize.scratch
    val gpus = 0 until requestSize.gpu toList

    ResourceAvailable(cpu, memory, scratch, gpus, None)
  }
}

class BatchCreateNodeFactory(
    batchConfig: BatchConfig,
    batch: BatchClient
) extends CreateNodeFactory {
  def apply(
      master: SimpleSocketAddress,
      masterPrefix: String,
      codeAddress: CodeAddress
  ) =
    new BatchCreateNode(
      masterAddress = master,
      masterPrefix = masterPrefix,
      codeAddress = codeAddress,
      batch = batch,
      batchConfig = batchConfig
    )
}

object BatchGetNodeName extends GetNodeName {
  def getNodeName(config: TasksConfig) = IO {
    val nodeName = config.nodeName
    if (nodeName.nonEmpty) RunningJobId(nodeName)
    else {
      val envJobId = Option(System.getenv("AWS_BATCH_JOB_ID"))
      RunningJobId(envJobId.getOrElse(java.net.InetAddress.getLocalHost.getHostName))
    }
  }
}

class BatchHostConfig(val config: BatchConfig) extends HostConfigurationFromConfig

class BatchConfig(val raw: Config) extends ConfigValuesForHostConfiguration {

  val region: String = raw.getString("tasks.elastic.batch.region")

  val jobQueue: String = raw.getString("tasks.elastic.batch.jobQueue")

  val jobDefinition: String = raw.getString("tasks.elastic.batch.jobDefinition")

  val minimumCpu: Int = raw.getInt("tasks.elastic.batch.minimumCpu")

  val minimumMemory: Int = raw.getInt("tasks.elastic.batch.minimumMemory")

  val tags: Map[String, String] = {
    val list: List[String] =
      raw.getStringList("tasks.elastic.batch.tags").asScala.toList
    list
      .grouped(2)
      .collect { case k :: v :: Nil => k -> v }
      .toMap
  }

}

object BatchElasticSupport {

  def apply(config: Option[Config]): cats.effect.Resource[IO, ElasticSupport] = {
    val batchConfig = new BatchConfig(tasks.util.loadConfig(config))
    cats.effect.Resource.make {
      IO {
        val batch =
          if (batchConfig.region.isEmpty) BatchClient.create
          else
            BatchClient.builder
              .region(Region.of(batchConfig.region))
              .build

        new ElasticSupport(
          hostConfig = Some(new BatchHostConfig(batchConfig)),
          shutdownFromNodeRegistry = new BatchShutdown(batch),
          shutdownFromWorker = new BatchShutdown(batch),
          createNodeFactory = new BatchCreateNodeFactory(batchConfig, batch),
          getNodeName = BatchGetNodeName
        )
      }
    } { _ => IO.unit }
  }
}
