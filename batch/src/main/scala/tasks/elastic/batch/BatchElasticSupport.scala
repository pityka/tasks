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

/** Records on-demand vCPUs that we have routed but AWS Batch has likely not yet
  * reflected in the compute environment's `desiredvCpus`. Combined with
  * `desiredvCpus` in [[BatchCreateNode.selectJobQueue]] so concurrent routing
  * decisions cannot all race past `onDemandMaxVcpu`.
  *
  * Entries are pruned by TTL — long enough for AWS Batch's autoscaler to bump
  * `desiredvCpus` to include the submission, typically 30-60s during scale-up.
  */
private[batch] class PendingOnDemandTracker(
    ttlNanos: Long,
    state: cats.effect.kernel.Ref[IO, Vector[(Long, Int)]]
) {

  /** Atomically prune entries older than `ttlNanos`, check whether `desired +
    * pendingSum + addCpu <= cap`, and if so append `(now, addCpu)`. Returns
    * whether the reservation was accepted and the pending sum observed
    * (post-prune, pre-add). The atomicity is provided by `Ref.modify`, so
    * concurrent callers can never both observe room when only one fits.
    */
  def tryReserve(
      now: Long,
      addCpu: Int,
      desiredVcpus: Int,
      cap: Int
  ): IO[(Boolean, Int)] = state.modify { entries =>
    val pruned = entries.filter { case (t, _) => (now - t) <= ttlNanos }
    val pendingSum = pruned.iterator.map(_._2).sum
    val accept = desiredVcpus + pendingSum + addCpu <= cap
    val next = if (accept) pruned :+ ((now, addCpu)) else pruned
    (next, (accept, pendingSum))
  }
}

private[batch] object PendingOnDemandTracker {
  def make(ttlNanos: Long): IO[PendingOnDemandTracker] =
    cats.effect.kernel.Ref
      .of[IO, Vector[(Long, Int)]](Vector.empty)
      .map(new PendingOnDemandTracker(ttlNanos, _))
}

class BatchCreateNode(
    masterAddress: SimpleSocketAddress,
    masterPrefix: String,
    codeAddress: CodeAddress,
    batch: BatchClient,
    batchConfig: BatchConfig,
    pendingOnDemand: PendingOnDemandTracker
) extends CreateNode {

  def requestOneNewJobFromJobScheduler(
      requestSize: ResourceRequest
  )(implicit
      config: TasksConfig
  ): IO[Either[String, (PendingJobId, ResourceAvailable)]] =
    for {
      selectedResources <- IO(selectResources(requestSize))
      targetQueue <- selectJobQueue(selectedResources)
      submitted <- IO.interruptible {
        Try {
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
            image = None,
            workerHealthUrlFile =
              config.workerHealthUrlFile.map(_.getAbsolutePath)
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
            .jobQueue(targetQueue)
            .jobDefinition(batchConfig.jobDefinition)
            .containerOverrides(containerOverrides)
            .tags(batchConfig.tags.asJava)
            .build

          val result = batch.submitJob(submitRequest)
          val jobId = PendingJobId(result.jobId)
          (jobId, selectedResources)
        }.toEither.left.map(_.getMessage)
      }
    } yield submitted

  private def selectJobQueue(selected: ResourceAvailable): IO[String] = {
    val decide: IO[String] =
      if (selected.gpu.nonEmpty) IO.pure(batchConfig.gpuJobQueue)
      else if (batchConfig.spotJobQueue == batchConfig.onDemandJobQueue)
        IO.pure(batchConfig.onDemandJobQueue)
      else {
        val queryCapacity = IO.interruptible {
          val computeEnvs =
            listComputeEnvironments(batchConfig.onDemandJobQueue)
          val (maxVcpus, desiredVcpus) = describeCapacity(computeEnvs)
          val cap = math.min(
            maxVcpus - batchConfig.onDemandHeadroomVcpu,
            batchConfig.onDemandMaxVcpu
          )
          (computeEnvs, maxVcpus, desiredVcpus, cap)
        }
        queryCapacity.attempt.flatMap {
          case Left(e) =>
            IO(
              scribe.warn(
                s"Failed to query on-demand queue capacity, defaulting to on-demand: ${e.getMessage}"
              )
            ).as(batchConfig.onDemandJobQueue)
          case Right((computeEnvs, maxVcpus, desiredVcpus, cap)) =>
            IO.monotonic.flatMap { nowFD =>
              pendingOnDemand
                .tryReserve(
                  now = nowFD.toNanos,
                  addCpu = selected.cpu,
                  desiredVcpus = desiredVcpus,
                  cap = cap
                )
                .flatMap { case (accepted, pendingSum) =>
                  val chosen =
                    if (accepted) batchConfig.onDemandJobQueue
                    else batchConfig.spotJobQueue
                  IO(
                    scribe.info(
                      s"on-demand queue ${batchConfig.onDemandJobQueue} CEs=[${computeEnvs.mkString(",")}] desired=$desiredVcpus pending=$pendingSum max=$maxVcpus headroom=${batchConfig.onDemandHeadroomVcpu} onDemandMaxVcpu=${batchConfig.onDemandMaxVcpu} cap=$cap ask=${selected.cpu} accepted=$accepted"
                    )
                  ).as(chosen)
                }
            }
        }
      }
    decide.flatTap(chosen =>
      IO(
        scribe.info(
          s"routing worker request cpu=${selected.cpu} gpu=${selected.gpu.size} -> queue=$chosen"
        )
      )
    )
  }

  private def listComputeEnvironments(jobQueueName: String): List[String] = {
    val resp = batch.describeJobQueues(
      DescribeJobQueuesRequest.builder.jobQueues(jobQueueName).build
    )
    resp.jobQueues.asScala.toList.flatMap { jq =>
      jq.computeEnvironmentOrder.asScala.toList.map(_.computeEnvironment)
    }
  }

  private def describeCapacity(
      computeEnvArns: List[String]
  ): (Int, Int) = {
    if (computeEnvArns.isEmpty) (0, 0)
    else {
      val resp = batch.describeComputeEnvironments(
        DescribeComputeEnvironmentsRequest.builder
          .computeEnvironments(computeEnvArns.asJava)
          .build
      )
      val ces = resp.computeEnvironments.asScala.toList
      val maxVcpus = ces.map { ce =>
        Option(ce.computeResources)
          .flatMap(cr => Option(cr.maxvCpus))
          .map(_.intValue)
          .getOrElse(0)
      }.sum
      val desiredVcpus = ces.map { ce =>
        Option(ce.computeResources)
          .flatMap(cr => Option(cr.desiredvCpus))
          .map(_.intValue)
          .getOrElse(0)
      }.sum
      (maxVcpus, desiredVcpus)
    }
  }

  override def convertRunningToPending(
      p: RunningJobId
  ): IO[Option[PendingJobId]] =
    IO.pure(Some(PendingJobId(p.value)))

  private def selectResources(
      requestSize: ResourceRequest
  ): ResourceAvailable = {
    val cpu = math.max(requestSize.cpu._2, batchConfig.minimumCpu)
    val memory = math.max(requestSize.memory, batchConfig.minimumMemory)
    val scratch = requestSize.scratch
    val gpus = 0 until requestSize.gpu toList

    ResourceAvailable(cpu, memory, scratch, gpus, None)
  }
}

class BatchCreateNodeFactory(
    batchConfig: BatchConfig,
    batch: BatchClient,
    pendingOnDemand: PendingOnDemandTracker
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
      batchConfig = batchConfig,
      pendingOnDemand = pendingOnDemand
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

  private def readQueue(path: String, fallback: String): String = {
    val v = if (raw.hasPath(path)) raw.getString(path) else ""
    if (v.nonEmpty) v else fallback
  }

  val gpuJobQueue: String =
    readQueue("tasks.elastic.batch.gpuJobQueue", jobQueue)

  val onDemandJobQueue: String =
    readQueue("tasks.elastic.batch.onDemandJobQueue", jobQueue)

  val spotJobQueue: String =
    readQueue("tasks.elastic.batch.spotJobQueue", onDemandJobQueue)

  val onDemandHeadroomVcpu: Int =
    if (raw.hasPath("tasks.elastic.batch.onDemandHeadroomVcpu"))
      raw.getInt("tasks.elastic.batch.onDemandHeadroomVcpu")
    else 0

  val onDemandMaxVcpu: Int =
    if (raw.hasPath("tasks.elastic.batch.onDemandMaxVcpu"))
      raw.getInt("tasks.elastic.batch.onDemandMaxVcpu")
    else Int.MaxValue

  val onDemandSubmissionWindow: scala.concurrent.duration.FiniteDuration =
    if (raw.hasPath("tasks.elastic.batch.onDemandSubmissionWindow"))
      scala.jdk.DurationConverters
        .JavaDurationOps(
          raw.getDuration("tasks.elastic.batch.onDemandSubmissionWindow")
        )
        .toScala
    else scala.concurrent.duration.DurationInt(60).seconds

  require(
    gpuJobQueue.nonEmpty && onDemandJobQueue.nonEmpty && spotJobQueue.nonEmpty,
    "At least one of tasks.elastic.batch.{jobQueue, gpuJobQueue, onDemandJobQueue, spotJobQueue} " +
      "must be set to a non-empty queue name or ARN. An empty value yields IAM errors like " +
      "\"not authorized on resource job-queue/\""
  )

  val jobDefinition: String = {
    val v = raw.getString("tasks.elastic.batch.jobDefinition")
    require(
      v.nonEmpty,
      "tasks.elastic.batch.jobDefinition must be set to a non-empty job-definition name or ARN. " +
        "An empty value yields IAM errors like \"not authorized on resource job-definition/\""
    )
    v
  }

  val minimumCpu: Int = raw.getInt("tasks.elastic.batch.minimumCpu")

  val minimumMemory: Int = raw.getInt("tasks.elastic.batch.minimumMemory")

  val logGroup: String = raw.getString("tasks.elastic.batch.logGroup")

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
    cats.effect.Resource.eval {
      for {
        batch <- IO {
          if (batchConfig.region.isEmpty) BatchClient.create
          else
            BatchClient.builder
              .region(Region.of(batchConfig.region))
              .build
        }
        pendingOnDemand <- PendingOnDemandTracker.make(
          ttlNanos = batchConfig.onDemandSubmissionWindow.toNanos
        )
      } yield new ElasticSupport(
        hostConfig = Some(new BatchHostConfig(batchConfig)),
        shutdownFromNodeRegistry = new BatchShutdown(batch),
        shutdownFromWorker = new BatchShutdown(batch),
        createNodeFactory =
          new BatchCreateNodeFactory(batchConfig, batch, pendingOnDemand),
        getNodeName = BatchGetNodeName
      )
    }
  }
}
