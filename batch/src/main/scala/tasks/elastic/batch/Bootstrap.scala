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

import org.ekrich.config.Config
import org.ekrich.config.ConfigFactory
import com.google.cloud.tools.jib.api.Containerizer
import com.google.cloud.tools.jib.api.LogEvent
import com.google.cloud.tools.jib.api.LogEvent.Level._

import software.amazon.awssdk.services.batch.BatchClient
import software.amazon.awssdk.services.batch.model._
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient
import software.amazon.awssdk.services.cloudwatchlogs.model._
import software.amazon.awssdk.regions.Region

import scala.jdk.CollectionConverters._
import cats.effect.IO
import cats.effect.ExitCode
import cats.effect.kernel.Resource
import tasks.TaskSystemComponents
import tasks.withTaskSystem
import tasks.fileservice.s3.S3Client

object Bootstrap {

  /** Entry point with 2 way branch:
    *   - 1. AWS_BATCH_JOB_ID env var is present then we are running inside AWS
    *     Batch. In that case we launch the tasksystem.
    *   - The tasksystem startup will decide whether it starts in app, queue or
    *     follower mode
    *   - The user of the tasksystem resource needs to act accordingly, in
    *     particular it should IO-block forever when it is a follower
    *   - 2. AWS_BATCH_JOB_ID not present then we run outside of Batch. Creates
    *     a container image, submits a Batch job, and trails its log output.
    *
    * @return
    *   If this is an App role then whatever the app produces (Right[T])
    *   otherwise if this is a bootstrap process or worker process then returns
    *   Left(ExitCode)
    *   - if this is a bootstrap process then the IO completes when the job
    *     completes
    *   - if this is a worker role then the IO never completes (job must be
    *     terminated by the main role)
    *   - if this is a master role then the IO completes when the useTs
    *     completes and the task system is closed
    */
  def entrypoint[T](
      containerizer: Containerizer,
      mainClassName: String,
      s3Resource: Resource[IO, Option[S3Client]] = Resource.pure(None),
      config: Option[Config] = None,
      batchRequestCpu: Int = 1,
      batchRequestMemoryMB: Int = 512
  )(
      useTs: TaskSystemComponents => IO[T]
  ): IO[Either[ExitCode, T]] = {

    val batchConfig = new BatchConfig(tasks.util.loadConfig(config))

    val batchJobId = Option(System.getenv("AWS_BATCH_JOB_ID"))

    if (batchJobId.isDefined) {
      scribe.info(
        s"AWS_BATCH_JOB_ID env found (${batchJobId.get}). Create task system."
      )

      val hostname =
        java.net.InetAddress.getLocalHost.getHostAddress

      val cfg0 = ConfigFactory.parseString(
        s"""
        hosts.hostname="$hostname"
        hosts.numCPU = 0
        tasks.worker-main-class = "$mainClassName"
        """
      )

      val cfg = cfg0.withFallback(tasks.util.loadConfig(config))
      withTaskSystem(
        cfg,
        s3Resource,
        BatchElasticSupport(Some(cfg)).map(Some(_))
      )(useTs)
    } else {
      scribe.info(
        "No AWS_BATCH_JOB_ID env found. Create container and submit Batch job of master."
      )

      val pathInContainer = "/tasksapp"
      val container = selfpackage.jib.containerize(
        out = addScribe(containerizer),
        mainClassNameArg = Some(mainClassName),
        pathInContainer = pathInContainer
      )

      scribe.info(
        s"Made container image with self package into ${container.getTargetImage()}"
      )

      val imageName = container.getTargetImage().toString

      val batch =
        if (batchConfig.region.isEmpty) BatchClient.create
        else
          BatchClient.builder
            .region(Region.of(batchConfig.region))
            .build

      val logs =
        if (batchConfig.region.isEmpty) CloudWatchLogsClient.create
        else
          CloudWatchLogsClient.builder
            .region(Region.of(batchConfig.region))
            .build

      val cpu = math.max(batchRequestCpu, batchConfig.minimumCpu)
      val memory = math.max(batchRequestMemoryMB, batchConfig.minimumMemory)

      val resourceReqs = List(
        ResourceRequirement.builder
          .`type`(ResourceType.VCPU)
          .value(cpu.toString)
          .build,
        ResourceRequirement.builder
          .`type`(ResourceType.MEMORY)
          .value(memory.toString)
          .build
      ).asJava

      val containerOverrides = ContainerOverrides.builder
        .resourceRequirements(resourceReqs)
        .command(
          "bash",
          s"$pathInContainer/entrypoint.sh"
        )
        .build

      val jobName =
        "tasks-master-" + java.util.UUID.randomUUID.toString.take(8)

      val submitRequest = SubmitJobRequest.builder
        .jobName(jobName)
        .jobQueue(batchConfig.jobQueue)
        .jobDefinition(batchConfig.jobDefinition)
        .containerOverrides(containerOverrides)
        .tags(batchConfig.tags.asJava)
        .build

      IO.interruptible {
        val result = batch.submitJob(submitRequest)
        val jobId = result.jobId
        scribe.info(s"Submitted Batch job $jobName with id $jobId")
        jobId
      }.flatMap { jobId =>
        pollJobAndStreamLogs(batch, logs, jobId, batchConfig).flatMap {
          exitCode =>
            IO {
              batch.close()
              logs.close()
              Left(exitCode)
            }
        }
      }
    }
  }

  private def pollJobAndStreamLogs(
      batch: BatchClient,
      logs: CloudWatchLogsClient,
      jobId: String,
      batchConfig: BatchConfig
  ): IO[ExitCode] = {

    def describeJob: IO[JobDetail] = IO.interruptible {
      val response = batch.describeJobs(
        DescribeJobsRequest.builder.jobs(jobId).build
      )
      response.jobs.asScala.head
    }

    def streamLogs(logStreamName: String): IO[Unit] = {
      val logGroupName =
        batchConfig.logGroup

      def fetchAndPrint(nextToken: Option[String]): IO[Option[String]] =
        IO.interruptible {
          val reqBuilder = GetLogEventsRequest.builder
            .logGroupName(logGroupName)
            .logStreamName(logStreamName)
            .startFromHead(true)

          nextToken.foreach(t => reqBuilder.nextToken(t))

          val response = logs.getLogEvents(reqBuilder.build)
          response.events.asScala.foreach { event =>
            print(event.message)
            if (!event.message.endsWith("\n")) println()
          }
          val fwd = response.nextForwardToken
          if (response.events.isEmpty) None
          else Some(fwd)
        }

      def loop(token: Option[String], jobDone: Boolean): IO[Unit] =
        fetchAndPrint(token).flatMap { newToken =>
          if (jobDone && newToken.isEmpty) IO.unit
          else if (jobDone)
            loop(newToken.orElse(token), jobDone)
          else
            IO.sleep(
              scala.concurrent.duration.FiniteDuration(3, "seconds")
            ) >> loop(newToken.orElse(token), jobDone = false)
        }

      loop(None, jobDone = false)
    }

    def waitForRunning: IO[Option[String]] = {
      describeJob.flatMap { job =>
        val status = job.statusAsString
        scribe.info(s"Job $jobId status: $status")
        status match {
          case "SUCCEEDED" | "FAILED" =>
            IO.pure(
              Option(job.container).flatMap(c => Option(c.logStreamName))
            )
          case "RUNNING" =>
            IO.pure(
              Option(job.container).flatMap(c => Option(c.logStreamName))
            )
          case _ =>
            IO.sleep(
              scala.concurrent.duration.FiniteDuration(5, "seconds")
            ) >> waitForRunning
        }
      }
    }

    def waitForCompletion: IO[ExitCode] = {
      describeJob.flatMap { job =>
        val status = job.statusAsString
        status match {
          case "SUCCEEDED" =>
            scribe.info(s"Job $jobId completed successfully")
            IO.pure(ExitCode(0))
          case "FAILED" =>
            val reason =
              Option(job.container).map(_.reason).getOrElse("unknown")
            scribe.error(s"Job $jobId failed: $reason")
            IO.pure(ExitCode(1))
          case _ =>
            IO.sleep(
              scala.concurrent.duration.FiniteDuration(5, "seconds")
            ) >> waitForCompletion
        }
      }
    }

    waitForRunning.flatMap {
      case None =>
        scribe.warn("No log stream available for job")
        waitForCompletion
      case Some(logStreamName) =>
        scribe.info(s"Streaming logs from $logStreamName")
        IO.both(streamLogs(logStreamName), waitForCompletion).flatMap {
          case (_, exitCode) =>
            streamLogs(logStreamName).as(exitCode)
        }
    }
  }

  private def addScribe(containerizer: Containerizer) = {
    containerizer.addEventHandler(
      classOf[LogEvent],
      (logEvent: LogEvent) =>
        logEvent.getLevel() match {
          case LIFECYCLE => scribe.info(logEvent.getMessage())
          case PROGRESS  => scribe.info(logEvent.getMessage())
          case ERROR     => scribe.error(logEvent.getMessage())
          case WARN      => scribe.warn(logEvent.getMessage())
          case DEBUG     => scribe.debug(logEvent.getMessage())
          case INFO      => scribe.info(logEvent.getMessage())
        }
    )
  }

}
