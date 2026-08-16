package tasks.elastic.batch

final case class BatchConfig(
    region: Option[String],
    jobQueue: Option[String],
    queues: List[String],
    jobDefinition: String,
    jobDefinitionsByImage: Map[String, String],
    minimumCpu: Int,
    minimumMemory: Int,
    logGroup: String,
    tags: Map[String, String]
) {

  require(
    jobDefinition.nonEmpty,
    "jobDefinition must be a non-empty job-definition name or ARN. " +
      "An empty value yields IAM errors like " +
      "\"not authorized on resource job-definition/\""
  )

  require(
    queues.forall(_.nonEmpty),
    "queues entries must be non-empty queue names or ARNs. " +
      "An empty value yields IAM errors like " +
      "\"not authorized on resource job-queue/\""
  )

  def withRegion(value: String): BatchConfig = copy(region = Some(value))

  def withJobQueue(value: String): BatchConfig = copy(jobQueue = Some(value))

  def withQueues(entries: String*): BatchConfig =
    copy(queues = queues ++ entries)

  def withJobDefinitionForImage(image: String, value: String): BatchConfig =
    copy(jobDefinitionsByImage = jobDefinitionsByImage + (image -> value))

  def withMinimumResources(cpu: Int, memoryMib: Int): BatchConfig =
    copy(minimumCpu = cpu, minimumMemory = memoryMib)

  def withLogGroup(value: String): BatchConfig = copy(logGroup = value)

  def withTags(entries: (String, String)*): BatchConfig =
    copy(tags = tags ++ entries)

  def resolveJobDefinition(image: Option[String]): Either[String, String] =
    image match {
      case None => Right(jobDefinition)
      case Some(img) =>
        jobDefinitionsByImage
          .get(img)
          .toRight(
            s"No AWS Batch job definition configured for image '$img'. " +
              "Register one with BatchConfig.withJobDefinitionForImage."
          )
    }

  override def toString: String =
    s"BatchConfig(region=${region.getOrElse("<default-chain>")}, " +
      s"jobQueue=${jobQueue.getOrElse("<none>")}, " +
      s"queues=[${queues.mkString(",")}], jobDefinition=$jobDefinition, " +
      s"imagesMapped=${jobDefinitionsByImage.size})"
}

object BatchConfig {

  def apply(jobDefinition: String, queues: List[String]): BatchConfig =
    BatchConfig(
      region = None,
      jobQueue = None,
      queues = queues,
      jobDefinition = jobDefinition,
      jobDefinitionsByImage = Map.empty,
      minimumCpu = 1,
      minimumMemory = 512,
      logGroup = "/aws/batch/job",
      tags = Map.empty
    )
}
