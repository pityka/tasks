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
import software.amazon.awssdk.services.ec2.Ec2Client
import software.amazon.awssdk.services.ec2.model.{
  DescribeInstanceTypesRequest,
  InstanceType => Ec2InstanceType
}
import software.amazon.awssdk.regions.Region

import scala.jdk.CollectionConverters._
import org.ekrich.config.Config
import cats.effect.IO
import cats.effect.Ref
import cats.effect.kernel.Deferred
import cats.effect.ExitCode
import cats.effect.std.Mutex
import tasks.util.message.Node

import java.time.{Instant, Duration => JDuration}
import scala.concurrent.duration._

class BatchShutdown(batch: BatchClient)
    extends ShutdownNode
    with ShutdownSelfNode {

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

case class InstanceCapacity(vcpus: Int, memoryMib: Int)

object BatchInstanceCapacity {

  def listComputeEnvironments(
      batch: BatchClient,
      jobQueueName: String
  ): IO[List[String]] =
    IO.interruptible {
      val resp = batch.describeJobQueues(
        DescribeJobQueuesRequest.builder.jobQueues(jobQueueName).build
      )
      resp.jobQueues.asScala.toList.flatMap { jq =>
        jq.computeEnvironmentOrder.asScala.toList.map(_.computeEnvironment)
      }
    }

  def listInstanceTypesForCEs(
      batch: BatchClient,
      computeEnvArns: List[String]
  ): IO[List[String]] =
    if (computeEnvArns.isEmpty) IO.pure(Nil)
    else
      IO.interruptible {
        val resp = batch.describeComputeEnvironments(
          DescribeComputeEnvironmentsRequest.builder
            .computeEnvironments(computeEnvArns.asJava)
            .build
        )
        resp.computeEnvironments.asScala.toList.flatMap { ce =>
          Option(ce.computeResources)
            .flatMap(cr => Option(cr.instanceTypes))
            .map(_.asScala.toList)
            .getOrElse(Nil)
        }.distinct
      }

  def describeInstanceTypeCapacities(
      ec2: Ec2Client,
      cache: Ref[IO, Map[String, InstanceCapacity]],
      names: List[String]
  ): IO[Map[String, InstanceCapacity]] = {
    val unique = names.distinct
    if (unique.isEmpty) IO.pure(Map.empty)
    else
      cache.get.flatMap { cached =>
        val missing = unique.filterNot(cached.contains)
        if (missing.isEmpty)
          IO.pure(cached.view.filterKeys(unique.toSet).toMap)
        else
          IO.interruptible {
            val resp = ec2.describeInstanceTypes(
              DescribeInstanceTypesRequest.builder
                .instanceTypes(missing.map(Ec2InstanceType.fromValue).asJava)
                .build
            )
            resp.instanceTypes.asScala.toList.map { it =>
              it.instanceTypeAsString -> InstanceCapacity(
                it.vCpuInfo.defaultVCpus,
                it.memoryInfo.sizeInMiB.toInt
              )
            }.toMap
          }.flatMap { fetched =>
            cache
              .update(_ ++ fetched)
              .as((cached ++ fetched).view.filterKeys(unique.toSet).toMap)
          }
      }
  }

  def largestInstanceForQueue(
      batch: BatchClient,
      ec2: Ec2Client,
      cache: Ref[IO, Map[String, InstanceCapacity]],
      queue: String
  ): IO[Option[InstanceCapacity]] =
    if (queue.isEmpty) IO.pure(None)
    else
      listComputeEnvironments(batch, queue).flatMap { ces =>
        listInstanceTypesForCEs(batch, ces).flatMap { types =>
          if (types.isEmpty || types.contains("optimal")) IO.pure(None)
          else
            describeInstanceTypeCapacities(ec2, cache, types).map { caps =>
              caps.values.foldLeft(Option.empty[InstanceCapacity]) {
                case (None, c) => Some(c)
                case (Some(acc), c) =>
                  Some(
                    InstanceCapacity(
                      math.max(acc.vcpus, c.vcpus),
                      math.max(acc.memoryMib, c.memoryMib)
                    )
                  )
              }
            }
        }
      }

  def adaptMinimums(
      batch: BatchClient,
      ec2: Ec2Client,
      cache: Ref[IO, Map[String, InstanceCapacity]],
      queue: String,
      configMinCpu: Int,
      configMinMemory: Int
  ): IO[(Int, Int)] =
    largestInstanceForQueue(batch, ec2, cache, queue)
      .map {
        case Some(cap) =>
          val effCpu = math.min(configMinCpu, cap.vcpus)
          val effMem = math.min(configMinMemory, cap.memoryMib)
          if (effCpu != configMinCpu || effMem != configMinMemory)
            scribe.info(
              s"clamping configured minimums (cpu=$configMinCpu, memMiB=$configMinMemory) " +
                s"to fit queue $queue largest instance (cpu=${cap.vcpus}, memMiB=${cap.memoryMib}): " +
                s"effective (cpu=$effCpu, memMiB=$effMem)"
            )
          (effCpu, effMem)
        case None =>
          (configMinCpu, configMinMemory)
      }
      .handleErrorWith { e =>
        IO(
          scribe.warn(
            s"Failed to derive CE-based minimums for queue $queue, using configured values: ${e.getMessage}"
          )
        ).as((configMinCpu, configMinMemory))
      }
}

class BatchCreateNode(
    masterAddress: SimpleSocketAddress,
    masterPrefix: String,
    codeAddress: CodeAddress,
    batch: BatchClient,
    ec2: Ec2Client,
    batchConfig: BatchConfig,
    requestMutex: Mutex[IO],
    recentOnDemandSubmissions: Ref[IO, List[(Instant, String)]],
    instanceTypeCache: Ref[IO, Map[String, InstanceCapacity]]
) extends CreateNode {

  def requestOneNewJobFromJobScheduler(
      requestSize: ResourceRequest
  )(implicit
      config: TasksConfig
  ): IO[Either[String, (PendingJobId, ResourceAvailable)]] =
    requestMutex.lock.surround {
      val preliminaryResources = selectResources(
        requestSize,
        batchConfig.minimumCpu,
        batchConfig.minimumMemory
      )
      selectJobQueue(preliminaryResources)
        .flatMap { targetQueue =>
          adaptMinimumsToQueue(targetQueue).map { case (minCpu, minMem) =>
            (targetQueue, selectResources(requestSize, minCpu, minMem))
          }
        }
        .flatMap { case (targetQueue, selectedResources) =>
          val submit = IO.interruptible {
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
          }
          val recordIfOnDemand =
            if (targetQueue == batchConfig.onDemandJobQueue)
              submit.flatTap { case (jobId, _) =>
                recordOnDemandSubmission(jobId.value)
              }
            else submit
          recordIfOnDemand
        }
        .attempt
        .map(_.left.map(_.getMessage))
    }

  private def recordOnDemandSubmission(jobId: String): IO[Unit] =
    IO.realTimeInstant.flatMap { now =>
      val cutoff = now.minus(JDuration.ofHours(1))
      recentOnDemandSubmissions.update { lst =>
        (now, jobId) :: lst.filter(_._1.isAfter(cutoff))
      }
    }

  private def recentOnDemandIds: IO[Set[String]] =
    IO.realTimeInstant.flatMap { now =>
      val cutoff = now.minus(JDuration.ofHours(1))
      recentOnDemandSubmissions.modify { lst =>
        val pruned = lst.filter(_._1.isAfter(cutoff))
        (pruned, pruned.map(_._2).toSet)
      }
    }

  private def dropRecentIds(ids: Set[String]): IO[Unit] =
    if (ids.isEmpty) IO.unit
    else
      recentOnDemandSubmissions.update(_.filterNot(t => ids.contains(t._2)))

  private def selectJobQueue(selected: ResourceAvailable): IO[String] = {
    val chosenIO: IO[String] =
      if (selected.gpu.nonEmpty) IO.pure(batchConfig.gpuJobQueue)
      else if (batchConfig.spotJobQueue == batchConfig.onDemandJobQueue)
        IO.pure(batchConfig.onDemandJobQueue)
      else {
        val onDemandHasRoom: IO[Boolean] = (for {
          computeEnvs <- listComputeEnvironments(batchConfig.onDemandJobQueue)
          maxVcpus <- describeMaxVcpus(computeEnvs)
          inUseVcpus <- sumOnDemandJobVcpus
          cap = math.min(
            maxVcpus - batchConfig.onDemandHeadroomVcpu,
            batchConfig.onDemandMaxVcpu
          )
          _ <- IO(
            scribe.info(
              s"on-demand queue ${batchConfig.onDemandJobQueue} CEs=[${computeEnvs.mkString(",")}] inUseVcpus=$inUseVcpus max=$maxVcpus headroom=${batchConfig.onDemandHeadroomVcpu} onDemandMaxVcpu=${batchConfig.onDemandMaxVcpu} cap=$cap ask=${selected.cpu}"
            )
          )
        } yield inUseVcpus + selected.cpu <= cap).handleErrorWith { e =>
          IO(
            scribe.warn(
              s"Failed to query on-demand queue capacity, defaulting to on-demand: ${e.getMessage}"
            )
          ).as(true)
        }
        onDemandHasRoom.map { onDemandHasRoom =>
          if (onDemandHasRoom) batchConfig.onDemandJobQueue
          else batchConfig.spotJobQueue
        }
      }
    chosenIO.flatTap { chosen =>
      IO(
        scribe.info(
          s"routing worker request cpu=${selected.cpu} gpu=${selected.gpu.size} -> queue=$chosen"
        )
      )
    }
  }

  private def listComputeEnvironments(
      jobQueueName: String
  ): IO[List[String]] =
    IO.interruptible {
      val resp = batch.describeJobQueues(
        DescribeJobQueuesRequest.builder.jobQueues(jobQueueName).build
      )
      resp.jobQueues.asScala.toList.flatMap { jq =>
        jq.computeEnvironmentOrder.asScala.toList.map(_.computeEnvironment)
      }
    }

  private def describeMaxVcpus(
      computeEnvArns: List[String]
  ): IO[Int] =
    if (computeEnvArns.isEmpty) IO.pure(0)
    else
      IO.interruptible {
        val resp = batch.describeComputeEnvironments(
          DescribeComputeEnvironmentsRequest.builder
            .computeEnvironments(computeEnvArns.asJava)
            .build
        )
        resp.computeEnvironments.asScala.toList.map { ce =>
          Option(ce.computeResources)
            .flatMap(cr => Option(cr.maxvCpus))
            .map(_.intValue)
            .getOrElse(0)
        }.sum
      }

  private val activeJobStatuses: List[JobStatus] = List(
    JobStatus.SUBMITTED,
    JobStatus.PENDING,
    JobStatus.RUNNABLE,
    JobStatus.STARTING,
    JobStatus.RUNNING
  )

  private def listJobIdsInStatus(
      queue: String,
      status: JobStatus
  ): IO[List[String]] = {
    def fetchPage(
        token: Option[String]
    ): IO[(List[String], Option[String])] =
      IO.interruptible {
        val builder = ListJobsRequest.builder
          .jobQueue(queue)
          .jobStatus(status)
        val req = token.fold(builder.build)(t => builder.nextToken(t).build)
        val resp = batch.listJobs(req)
        val ids = resp.jobSummaryList.asScala.toList.map(_.jobId)
        (ids, Option(resp.nextToken))
      }

    def loop(acc: List[String], token: Option[String]): IO[List[String]] =
      fetchPage(token).flatMap { case (ids, next) =>
        val updated = acc ::: ids
        next match {
          case Some(_) => loop(updated, next)
          case None    => IO.pure(updated)
        }
      }

    loop(Nil, None)
  }

  private def listAllActiveJobIds: IO[List[String]] = {
    val queue = batchConfig.onDemandJobQueue
    activeJobStatuses.foldLeft(IO.pure(List.empty[String])) {
      (accIO, status) =>
        accIO.flatMap { acc =>
          listJobIdsInStatus(queue, status).map(acc ::: _)
        }
    }
  }

  private case class JobDetail(
      id: String,
      status: JobStatus,
      vcpus: Int
  )

  private def describeJobs(jobIds: List[String]): IO[List[JobDetail]] =
    if (jobIds.isEmpty) IO.pure(Nil)
    else
      IO.interruptible {
        jobIds.grouped(100).flatMap { batchIds =>
          val resp = batch.describeJobs(
            DescribeJobsRequest.builder.jobs(batchIds.asJava).build
          )
          resp.jobs.asScala.toList.map { j =>
            val vcpus = Option(j.container)
              .flatMap(c => Option(c.resourceRequirements))
              .map(_.asScala.toList)
              .getOrElse(Nil)
              .find(_.`type` == ResourceType.VCPU)
              .flatMap(r => Option(r.value))
              .map(_.toInt)
              .getOrElse(0)
            JobDetail(j.jobId, j.status, vcpus)
          }
        }.toList
      }

  private val maxReconcileAttempts: Int = 5
  private val reconcileBackoff: FiniteDuration = 1500.millis

  private def sumOnDemandJobVcpus: IO[Int] = {
    def loop(attempt: Int): IO[Int] =
      for {
        activeIds <- listAllActiveJobIds.map(_.toSet)
        recentIds <- recentOnDemandIds
        missing = recentIds -- activeIds
        result <-
          if (missing.isEmpty) describeJobs(activeIds.toList).map(_.map(_.vcpus).sum)
          else
            describeJobs(missing.toList).flatMap { details =>
              val terminal =
                details.filter(d => !activeJobStatuses.contains(d.status))
              val stillActive =
                details.filter(d => activeJobStatuses.contains(d.status))
              val terminalIds = terminal.map(_.id).toSet
              val unknownIds = missing -- details.map(_.id).toSet
              dropRecentIds(terminalIds) *> {
                if (stillActive.isEmpty && unknownIds.isEmpty)
                  describeJobs(activeIds.toList).map(_.map(_.vcpus).sum)
                else if (attempt >= maxReconcileAttempts)
                  IO(
                    scribe.warn(
                      s"listJobs eventual consistency: ${stillActive.size} active + ${unknownIds.size} unknown ids still missing after $maxReconcileAttempts attempts; counting them optimistically"
                    )
                  ) *> describeJobs(activeIds.toList).map { ds =>
                    ds.map(_.vcpus).sum + stillActive.map(_.vcpus).sum
                  }
                else IO.sleep(reconcileBackoff) *> loop(attempt + 1)
              }
            }
      } yield result

    loop(0)
  }

  override def convertRunningToPending(
      p: RunningJobId
  ): IO[Option[PendingJobId]] =
    IO.pure(Some(PendingJobId(p.value)))

  private def selectResources(
      requestSize: ResourceRequest,
      minCpu: Int,
      minMemory: Int
  ): ResourceAvailable = {
    val cpu = math.max(requestSize.cpu._2, minCpu)
    val memory = math.max(requestSize.memory, minMemory)
    val scratch = requestSize.scratch
    val gpus = 0 until requestSize.gpu toList

    ResourceAvailable(cpu, memory, scratch, gpus, None)
  }

  private def adaptMinimumsToQueue(queue: String): IO[(Int, Int)] =
    BatchInstanceCapacity.adaptMinimums(
      batch,
      ec2,
      instanceTypeCache,
      queue,
      batchConfig.minimumCpu,
      batchConfig.minimumMemory
    )
}

class BatchCreateNodeFactory(
    batchConfig: BatchConfig,
    batch: BatchClient,
    ec2: Ec2Client,
    requestMutex: Mutex[IO],
    recentOnDemandSubmissions: Ref[IO, List[(Instant, String)]],
    instanceTypeCache: Ref[IO, Map[String, InstanceCapacity]]
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
      ec2 = ec2,
      batchConfig = batchConfig,
      requestMutex = requestMutex,
      recentOnDemandSubmissions = recentOnDemandSubmissions,
      instanceTypeCache = instanceTypeCache
    )
}

object BatchGetNodeName extends GetNodeName {
  def getNodeName(config: TasksConfig) = IO {
    val nodeName = config.nodeName
    if (nodeName.nonEmpty) RunningJobId(nodeName)
    else {
      val envJobId = Option(System.getenv("AWS_BATCH_JOB_ID"))
      RunningJobId(
        envJobId.getOrElse(java.net.InetAddress.getLocalHost.getHostName)
      )
    }
  }
}

class BatchHostConfig(val config: BatchConfig)
    extends HostConfigurationFromConfig

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

  def apply(
      config: Option[Config]
  ): cats.effect.Resource[IO, ElasticSupport] = {
    val batchConfig = new BatchConfig(tasks.util.loadConfig(config))
    cats.effect.Resource.eval {
      for {
        requestMutex <- Mutex[IO]
        recentOnDemandSubmissions <- Ref.of[IO, List[(Instant, String)]](Nil)
        instanceTypeCache <- Ref.of[IO, Map[String, InstanceCapacity]](Map.empty)
        support <- IO {
          val batch =
            if (batchConfig.region.isEmpty) BatchClient.create
            else
              BatchClient.builder
                .region(Region.of(batchConfig.region))
                .build

          val ec2 =
            if (batchConfig.region.isEmpty) Ec2Client.create
            else
              Ec2Client.builder
                .region(Region.of(batchConfig.region))
                .build

          new ElasticSupport(
            hostConfig = Some(new BatchHostConfig(batchConfig)),
            shutdownFromNodeRegistry = new BatchShutdown(batch),
            shutdownFromWorker = new BatchShutdown(batch),
            createNodeFactory = new BatchCreateNodeFactory(
              batchConfig,
              batch,
              ec2,
              requestMutex,
              recentOnDemandSubmissions,
              instanceTypeCache
            ),
            getNodeName = BatchGetNodeName
          )
        }
      } yield support
    }
  }
}
