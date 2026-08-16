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
import software.amazon.awssdk.services.ec2.model.DescribeInstanceTypesRequest
import software.amazon.awssdk.regions.Region

import scala.jdk.CollectionConverters._
import cats.effect.IO
import cats.effect.Ref
import cats.effect.kernel.Deferred
import cats.effect.ExitCode
import cats.effect.std.Mutex
import cats.syntax.parallel._
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

case class InstanceCapacity(vcpus: Int, memoryMib: Int, gpus: Int)

case class BatchQueueInfo(
    name: String,
    spot: Boolean,
    maxVcpus: Int,
    instances: List[InstanceCapacity]
)

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

  private case class ComputeEnvironmentDetails(
      provisioning: CRType,
      maxVcpus: Int,
      instanceTypes: List[String]
  )

  private def describeComputeEnvironmentDetails(
      batch: BatchClient,
      computeEnvArns: List[String]
  ): IO[List[ComputeEnvironmentDetails]] =
    if (computeEnvArns.isEmpty) IO.pure(Nil)
    else
      IO.interruptible {
        val response = batch.describeComputeEnvironments(
          DescribeComputeEnvironmentsRequest.builder
            .computeEnvironments(computeEnvArns.asJava)
            .build
        )
        response.computeEnvironments.asScala.toList.map { computeEnv =>
          val computeResources = Option(computeEnv.computeResources)
          val provisioning = computeResources
            .map(_.`type`)
            .getOrElse(CRType.UNKNOWN_TO_SDK_VERSION)
          val maxVcpus = computeResources
            .flatMap(cr => Option(cr.maxvCpus))
            .map(_.intValue)
            .getOrElse(0)
          val instanceTypes = computeResources
            .flatMap(cr => Option(cr.instanceTypes))
            .map(_.asScala.toList)
            .getOrElse(Nil)
            .filter(t => t != null && t.nonEmpty)
          ComputeEnvironmentDetails(provisioning, maxVcpus, instanceTypes)
        }
      }

  def listInstanceTypesForCEs(
      batch: BatchClient,
      computeEnvArns: List[String]
  ): IO[List[String]] =
    describeComputeEnvironmentDetails(batch, computeEnvArns)
      .map(_.flatMap(_.instanceTypes).distinct)

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
                .instanceTypesWithStrings(missing.asJava)
                .build
            )
            resp.instanceTypes.asScala.toList.map { instanceType =>
              val gpuCount = Option(instanceType.gpuInfo)
                .map(_.gpus.asScala.toList.map(_.count.intValue).sum)
                .getOrElse(0)
              instanceType.instanceTypeAsString -> InstanceCapacity(
                instanceType.vCpuInfo.defaultVCpus,
                instanceType.memoryInfo.sizeInMiB.toInt,
                gpuCount
              )
            }.toMap
          }.flatMap { fetched =>
            cache
              .update(_ ++ fetched)
              .as((cached ++ fetched).view.filterKeys(unique.toSet).toMap)
          }
      }
  }

  def describeQueueInfo(
      batch: BatchClient,
      ec2: Ec2Client,
      cache: Ref[IO, Map[String, InstanceCapacity]],
      queue: String
  ): IO[BatchQueueInfo] =
    listComputeEnvironments(batch, queue).flatMap { computeEnvArns =>
      if (computeEnvArns.isEmpty)
        IO.raiseError(
          new RuntimeException(
            s"AWS Batch queue '$queue' resolves to no compute environments. " +
              "The queue may not exist, may be misspelled, or may have no CEs attached. " +
              "Fix BatchConfig.queues."
          )
        )
      else
        describeComputeEnvironmentDetails(batch, computeEnvArns).flatMap {
          computeEnvDetails =>
            val usesSpot = computeEnvDetails.exists(ce =>
              ce.provisioning == CRType.SPOT ||
                ce.provisioning == CRType.FARGATE_SPOT
            )
            val queueMaxVcpus = computeEnvDetails.map(_.maxVcpus).sum
            val allInstanceTypes =
              computeEnvDetails.flatMap(_.instanceTypes).distinct
            val instancesIO: IO[List[InstanceCapacity]] =
              if (
                allInstanceTypes.isEmpty || allInstanceTypes.contains("optimal")
              ) IO.pure(Nil)
              else
                describeInstanceTypeCapacities(ec2, cache, allInstanceTypes)
                  .map(_.values.toList)
            instancesIO.map(BatchQueueInfo(queue, usesSpot, queueMaxVcpus, _))
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
      describeQueueInfo(batch, ec2, cache, queue).map { info =>
        info.instances.foldLeft(Option.empty[InstanceCapacity]) {
          case (None, c) => Some(c)
          case (Some(acc), c) =>
            Some(
              InstanceCapacity(
                math.max(acc.vcpus, c.vcpus),
                math.max(acc.memoryMib, c.memoryMib),
                math.max(acc.gpus, c.gpus)
              )
            )
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

object BatchCreateNode {

  val queueLabelPrefix: String = "aws-batch-queue:"

  private[batch] def canHostRequest(
      queue: BatchQueueInfo,
      request: ResourceAvailable
  ): Boolean =
    queue.instances.isEmpty ||
      queue.instances.exists(instance =>
        instance.vcpus >= request.cpu &&
          instance.memoryMib >= request.memory &&
          instance.gpus >= request.gpu.size
      )

  private[batch] def largestInstanceVcpus(queue: BatchQueueInfo): Int =
    if (queue.instances.isEmpty) Int.MaxValue
    else queue.instances.map(_.vcpus).max

  private[batch] def chooseQueue(
      queueInfos: List[BatchQueueInfo],
      request: ResourceAvailable,
      onDemandHasRoom: Boolean
  ): Option[BatchQueueInfo] = {
    val fitting = queueInfos.filter(canHostRequest(_, request))
    val routed =
      if (request.gpu.nonEmpty) fitting
      else {
        val (onDemand, spot) = fitting.partition(!_.spot)
        val preferred = if (onDemandHasRoom) onDemand else spot
        val fallback = if (onDemandHasRoom) spot else onDemand
        if (preferred.nonEmpty) preferred else fallback
      }
    routed.sortBy(largestInstanceVcpus).headOption
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
    batchConfig.resolveJobDefinition(requestSize.image) match {
      case Left(err) => IO.pure(Left(err))
      case Right(jobDefinition) => submitJob(requestSize, jobDefinition)
    }

  private def submitJob(
      requestSize: ResourceRequest,
      jobDefinition: String
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
        .flatMap { targetQueueInfo =>
          adaptMinimumsToQueue(targetQueueInfo.name).map { case (minCpu, minMem) =>
            (targetQueueInfo, selectResources(requestSize, minCpu, minMem))
          }
        }
        .flatMap { case (targetQueueInfo, selectedResources) =>
          val targetQueue = targetQueueInfo.name
          val labeledResources = selectedResources.copy(
            labels = selectedResources.labels +
              s"${BatchCreateNode.queueLabelPrefix}$targetQueue"
          )
          val submit = IO.interruptible {
            val script = Deployment.script(
              memory = labeledResources.memory,
              cpu = labeledResources.cpu,
              scratch = labeledResources.scratch,
              gpus = labeledResources.gpu,
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
              image = requestSize.image,
              workerHealthUrlFile =
                config.workerHealthUrlFile.map(_.getAbsolutePath),
              labels = labeledResources.labels
            )(config)

            val resourceReqs = {
              val reqs = List(
                ResourceRequirement.builder
                  .`type`(ResourceType.VCPU)
                  .value(labeledResources.cpu.toString)
                  .build,
                ResourceRequirement.builder
                  .`type`(ResourceType.MEMORY)
                  .value(labeledResources.memory.toString)
                  .build
              ) ++ (if (labeledResources.gpu.nonEmpty)
                      List(
                        ResourceRequirement.builder
                          .`type`(ResourceType.GPU)
                          .value(labeledResources.gpu.size.toString)
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
              .jobDefinition(jobDefinition)
              .containerOverrides(containerOverrides)
              .tags(batchConfig.tags.asJava)
              .build

            val result = batch.submitJob(submitRequest)
            val jobId = PendingJobId(result.jobId)
            (jobId, labeledResources)
          }
          val recordIfOnDemand =
            if (!targetQueueInfo.spot)
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

  private def describeWorkerQueues: IO[List[BatchQueueInfo]] =
    batchConfig.queues.parTraverse { queueName =>
      BatchInstanceCapacity
        .describeQueueInfo(batch, ec2, instanceTypeCache, queueName)
    }

  private def onDemandHasAggregateHeadroom(
      onDemandCandidates: List[BatchQueueInfo],
      request: ResourceAvailable
  ): IO[Boolean] =
    if (onDemandCandidates.isEmpty) IO.pure(false)
    else {
      val aggregateMaxVcpus = onDemandCandidates.map(_.maxVcpus.toLong).sum
      sumOnDemandJobVcpus(onDemandCandidates.map(_.name)).map { inUseVcpus =>
        scribe.info(
          s"on-demand aggregate cap=$aggregateMaxVcpus inUseVcpus=$inUseVcpus ask=${request.cpu} queues=[${onDemandCandidates.map(_.name).mkString(",")}]"
        )
        inUseVcpus.toLong + request.cpu.toLong <= aggregateMaxVcpus
      }.handleErrorWith { e =>
        IO(
          scribe.warn(
            s"Failed to compute on-demand headroom, defaulting to on-demand: ${e.getMessage}"
          )
        ).as(true)
      }
    }

  private def selectJobQueue(
      request: ResourceAvailable
  ): IO[BatchQueueInfo] =
    describeWorkerQueues.flatMap { queueInfos =>
      val fittingOnDemand = queueInfos
        .filter(BatchCreateNode.canHostRequest(_, request))
        .filter(!_.spot)
      val hasRoomIO =
        if (request.gpu.nonEmpty || fittingOnDemand.isEmpty) IO.pure(false)
        else onDemandHasAggregateHeadroom(fittingOnDemand, request)
      hasRoomIO.flatMap { hasRoom =>
        BatchCreateNode.chooseQueue(queueInfos, request, hasRoom) match {
          case Some(chosen) =>
            IO(
              scribe.info(
                s"routing worker request cpu=${request.cpu} gpu=${request.gpu.size} memMiB=${request.memory} -> queue=${chosen.name} spot=${chosen.spot}"
              )
            ).as(chosen)
          case None =>
            IO.raiseError(
              new RuntimeException(
                s"No Batch queue in BatchConfig.queues can host request cpu=${request.cpu} memMiB=${request.memory} gpus=${request.gpu.size}. Configured queues=[${batchConfig.queues.mkString(",")}]"
              )
            )
        }
      }
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

  private def listActiveJobIds(queues: List[String]): IO[List[String]] =
    queues
      .parTraverse { queue =>
        activeJobStatuses
          .parTraverse(status => listJobIdsInStatus(queue, status))
          .map(_.flatten)
      }
      .map(_.flatten)

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

  private def sumOnDemandJobVcpus(onDemandQueues: List[String]): IO[Int] = {
    if (onDemandQueues.isEmpty) IO.pure(0)
    else {
      def loop(attempt: Int): IO[Int] =
        for {
          activeIds <- listActiveJobIds(onDemandQueues).map(_.toSet)
          recentIds <- recentOnDemandIds
          missing = recentIds -- activeIds
          result <-
            if (missing.isEmpty)
              describeJobs(activeIds.toList).map(_.map(_.vcpus).sum)
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

    ResourceAvailable(cpu, memory, scratch, gpus, requestSize.image)
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

object BatchElasticSupport {

  def apply(
      batchConfig: BatchConfig
  ): cats.effect.Resource[IO, ElasticSupport] = {
    cats.effect.Resource.eval {
      for {
        requestMutex <- Mutex[IO]
        recentOnDemandSubmissions <- Ref.of[IO, List[(Instant, String)]](Nil)
        instanceTypeCache <- Ref.of[IO, Map[String, InstanceCapacity]](Map.empty)
        support <- IO {
          val batch =
            batchConfig.region.fold(BatchClient.create)(region =>
              BatchClient.builder
                .region(Region.of(region))
                .build
            )

          val ec2 =
            batchConfig.region.fold(Ec2Client.create)(region =>
              Ec2Client.builder
                .region(Region.of(region))
                .build
            )

          scribe.info(s"AWS Batch elastic backend: $batchConfig")

          new ElasticSupport(
            hostConfig = Some((tasksConfig: TasksConfig) =>
              new DefaultHostConfigurationFromConfig()(tasksConfig)
            ),
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
