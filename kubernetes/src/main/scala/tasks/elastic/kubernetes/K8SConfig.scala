package tasks.elastic.kubernetes

import io.k8s.api.core.v1.PodSpec

final case class K8SConfig(
    image: Option[String],
    imageApplicationSubPath: String,
    namespace: String,
    imagePullPolicy: String,
    podSpec: Option[PodSpec],
    hostNameOrIPEnvVar: String,
    cpuLimitEnvVar: String,
    ramLimitEnvVar: String,
    scratchLimitEnvVar: String,
    extraCpu: Int,
    extraRam: Int,
    minimumCpu: Int,
    minimumRam: Int
) {

  require(namespace.nonEmpty, "namespace must be a non-empty namespace name")

  require(
    imageApplicationSubPath.nonEmpty,
    "imageApplicationSubPath must be a non-empty path inside the image"
  )

  def withImage(value: String): K8SConfig = copy(image = Some(value))

  def withImageApplicationSubPath(value: String): K8SConfig =
    copy(imageApplicationSubPath = value)

  def withNamespace(value: String): K8SConfig = copy(namespace = value)

  def withImagePullPolicy(value: String): K8SConfig =
    copy(imagePullPolicy = value)

  def withPodSpec(value: PodSpec): K8SConfig = copy(podSpec = Some(value))

  def withEnvVarNames(
      hostNameOrIP: String,
      cpuLimit: String,
      ramLimit: String,
      scratchLimit: String
  ): K8SConfig =
    copy(
      hostNameOrIPEnvVar = hostNameOrIP,
      cpuLimitEnvVar = cpuLimit,
      ramLimitEnvVar = ramLimit,
      scratchLimitEnvVar = scratchLimit
    )

  def withExtraLimits(cpu: Int, ram: Int): K8SConfig =
    copy(extraCpu = cpu, extraRam = ram)

  def withMinimumLimits(cpu: Int, ram: Int): K8SConfig =
    copy(minimumCpu = cpu, minimumRam = ram)

  def ownsNodeId(nodeId: String): Boolean =
    nodeId.startsWith(namespace + "/")

  def resolveImage(requested: Option[String]): Either[String, String] =
    requested
      .filter(_.nonEmpty)
      .orElse(image.filter(_.nonEmpty))
      .toRight(
        "No container image for the Kubernetes worker pod: set it with " +
          "K8SConfig.withImage or with ResourceRequest.image."
      )

  override def toString: String =
    s"K8SConfig(image=${image.getOrElse("<from-request>")}, " +
      s"namespace=$namespace, imagePullPolicy=$imagePullPolicy, " +
      s"podSpecTemplate=${podSpec.isDefined})"
}

object K8SConfig {

  def apply(): K8SConfig =
    K8SConfig(
      image = None,
      imageApplicationSubPath = "/tasksapp",
      namespace = "default",
      imagePullPolicy = "IfNotPresent",
      podSpec = None,
      hostNameOrIPEnvVar = "TASKS_K8S_MY_POD_IP",
      cpuLimitEnvVar = "TASKS_K8S_MY_CPU_LIMIT",
      ramLimitEnvVar = "TASKS_K8S_MY_RAM_LIMIT",
      scratchLimitEnvVar = "TASKS_K8S_MY_SCRATCH_LIMIT",
      extraCpu = 0,
      extraRam = 500,
      minimumCpu = 1,
      minimumRam = 500
    )
}
