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

package tasks.elastic.kubernetes

import scala.util._

import tasks.elastic._
import tasks.shared._
import tasks.util.config._
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.fabric8.kubernetes.api.model.Quantity
import tasks.deploy.HostConfigurationFromConfig
import tasks.util.Uri
import tasks.util.SimpleSocketAddress
import io.fabric8.kubernetes.api.model.TolerationBuilder

class K8SShutdown(k8s: KubernetesClient)
    extends ShutdownNode {
  def shutdownRunningNode(nodeName: RunningJobId): Unit = {
    scribe.info(s"Shut down $nodeName")
    val spl = nodeName.value.split("/")
    val ns = spl(0)
    val podName = spl(1)
    k8s.pods.inNamespace(ns).withName(podName).delete()
  }

  def shutdownPendingNode(nodeName: PendingJobId): Unit = {
    scribe.info(s"Shut down $nodeName")
    val spl = nodeName.value.split("/")
    val ns = spl(0)
    val podName = spl(1)
    k8s.pods.inNamespace(ns).withName(podName).delete()
  }

}

object KubernetesHelpers {
  def newName = Random.alphanumeric.take(128).mkString.toLowerCase
}

class K8SCreateNode(
    masterAddress: SimpleSocketAddress,
    codeAddress: CodeAddress,
    k8s: KubernetesClient
)(implicit config: TasksConfig, elasticSupport: ElasticSupportFqcn)
    extends CreateNode {

  def requestOneNewJobFromJobScheduler(
      requestSize: ResourceRequest
  ): Try[(PendingJobId, ResourceAvailable)] = {

    val userCPURequest = math.max(requestSize.cpu._2, config.kubernetesCpuMin)
    val userRamRequest = math.max(requestSize.memory, config.kubernetesRamMin)

    val kubeCPURequest = userCPURequest + config.kubernetesCpuExtra
    val kubeRamRequest = userRamRequest + config.kubernetesRamExtra

    val script = Deployment.script(
      memory = userRamRequest,
      cpu = userCPURequest,
      scratch = requestSize.scratch,
      gpus = 0 until requestSize.gpu toList,
      elasticSupport = elasticSupport,
      masterAddress = masterAddress,
      download = Uri(
        scheme = "http",
        hostname = codeAddress.address.getHostName,
        port = codeAddress.address.getPort,
        path = "/"
      ),
      slaveHostname = None,
      background = false
    )

    val command = Seq("/bin/bash", "-c", script)

    val podName = KubernetesHelpers.newName
    val jobName = config.kubernetesNamespace + "/" + podName

    val imageName = config.kubernetesImageName

    val gpuTaintTolerations = config.kubernetesGpuTaintToleration.map {
      case (effect, key, operator, seconds, value) =>
        val builder0 = (new TolerationBuilder)
          .withKey(key)
          .withEffect(effect)
          .withOperator(operator)
        val builder1 = operator match {
          case "Exists" =>
            builder0
          case "Equal" =>
            builder0.withValue(value)
          case _ =>
            scribe.info(s"Unknown operator $operator")
            builder0
        }

        val builder2 = effect match {
          case "NoExecute" =>
            val s = seconds.toLong
            if (s >= 0) builder1.withTolerationSeconds(s)
            else builder1
          case _ => builder1
        }

        builder2.build
    }

    import scala.jdk.CollectionConverters._
    Try {
      k8s.pods
        .inNamespace(config.kubernetesNamespace)
        .resource(
          new PodBuilder().withNewMetadata
            .withName(podName)
            .endMetadata()
            .withNewSpec()
            .addNewContainer
            .withImage(imageName)
            .withCommand(command.asJava)
            .withName("tasks-worker")
            .withImagePullPolicy(config.kubernetesImagePullPolicy)
            //  IP
            .addNewEnv()
            .withName(config.kubernetesHostNameOrIPEnvVar)
            .withNewValueFrom()
            .withNewFieldRef()
            .withFieldPath("status.podIP")
            .endFieldRef()
            .endValueFrom()
            .endEnv()
            //  CPU
            .addNewEnv()
            .withName(config.kubernetesCpuLimitEnvVar)
            .withNewValueFrom()
            .withNewResourceFieldRef()
            .withContainerName("tasks-worker")
            .withResource("limits.cpu")
            .endResourceFieldRef()
            .endValueFrom()
            .endEnv()
            //  RAM
            .addNewEnv()
            .withName(config.kubernetesRamLimitEnvVar)
            .withNewValueFrom()
            .withNewResourceFieldRef()
            .withContainerName("tasks-worker")
            .withResource("limits.memory")
            .endResourceFieldRef()
            .endValueFrom()
            .endEnv()
            //  SCRATCH
            .addNewEnv()
            .withName(config.kubernetesScratchLimitEnvVar)
            .withNewValueFrom()
            .withNewResourceFieldRef()
            .withContainerName("tasks-worker")
            .withResource("limits.ephemeral-storage")
            .endResourceFieldRef()
            .endValueFrom()
            .endEnv()
            .addNewEnv()
            .withName("TASKS_JOB_NAME")
            .withValue(jobName)
            .endEnv()
            .withNewResources()
            .withRequests(
              (Map(
                "cpu" ->
                  new Quantity(kubeCPURequest.toString),
                "memory" -> new Quantity(s"${kubeRamRequest}M")
              ) ++ (if (requestSize.scratch > 0)
                      Map(
                        "ephemeral-storage" -> new Quantity(
                          s"${requestSize.scratch.toString}M"
                        )
                      )
                    else Map.empty)).asJava
            )
            .withLimits(
              (if (requestSize.gpu > 0)
                 Map(
                   "nvidia.com/gpu" -> new Quantity(
                     requestSize.gpu.toString
                   )
                 )
               else Map.empty[String, Quantity]).asJava
            )
            .endResources()
            .endContainer
            .withRestartPolicy("Never")
            .withTolerations(
              (if (requestSize.gpu > 0) gpuTaintTolerations else Nil).asJava
            )
            .endSpec
            .build()
        )
        .create()

      val available = ResourceAvailable(
        cpu = userCPURequest,
        memory = userRamRequest,
        scratch = requestSize.scratch,
        gpu = 0 until requestSize.gpu toList
      )

      (PendingJobId(jobName), available)
    }

  }

}

class K8SCreateNodeFactory(k8s: KubernetesClient)(implicit
    config: TasksConfig,
    fqcn: ElasticSupportFqcn
) extends CreateNodeFactory {
  def apply(master: SimpleSocketAddress, codeAddress: CodeAddress) =
    new K8SCreateNode(master, codeAddress, k8s)
}

object K8SGetNodeName extends GetNodeName {
  def getNodeName = System.getenv("TASKS_JOB_NAME")
}

object K8SElasticSupport extends ElasticSupportFromConfig {
  implicit val fqcn : ElasticSupportFqcn = ElasticSupportFqcn(
    "tasks.elastic.kubernetes.K8SElasticSupport"
  )
  def apply(implicit config: TasksConfig) = {
    val k8s =
      (new KubernetesClientBuilder).build
    SimpleElasticSupport(
      fqcn = fqcn,
      hostConfig = Some(new K8SHostConfig()),
      reaperFactory = None,
      shutdown = new K8SShutdown(k8s),
      createNodeFactory = new K8SCreateNodeFactory(k8s),
      getNodeName = K8SGetNodeName
    )
  }
}

trait K8SHostConfigurationImpl extends HostConfigurationFromConfig {

  implicit def config: TasksConfig

  private lazy val myhostname =
    Option(System.getenv(config.kubernetesHostNameOrIPEnvVar))
      .getOrElse(config.hostName)

  override lazy val myAddress = SimpleSocketAddress(myhostname, myPort)

  override lazy val availableMemory = Option(
    System.getenv(config.kubernetesRamLimitEnvVar)
  ).map(v => (v.toLong / 1000000).toInt).getOrElse(config.hostRAM)

  override lazy val availableScratch = Option(
    System.getenv(config.kubernetesScratchLimitEnvVar)
  ).map(v => (v.toLong / 1000000).toInt).getOrElse(config.hostScratch)

  override lazy val availableCPU =
    Option(System.getenv(config.kubernetesCpuLimitEnvVar))
      .map(_.toInt)
      .getOrElse(config.hostNumCPU)

}

class K8SHostConfig(implicit val config: TasksConfig)
    extends K8SHostConfigurationImpl
