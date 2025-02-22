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

import tasks.deploy.HostConfigurationFromConfig
import tasks.util.Uri
import tasks.util.SimpleSocketAddress
import cats.effect._
import com.goyeau.kubernetes.client._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scala.concurrent.duration._

import cats.effect.unsafe.implicits.global
import io.k8s.api.core.v1.Toleration
import io.k8s.api.core.v1.Pod
import io.k8s.api.core.v1.PodSpec
import io.k8s.api.core.v1.Container
import io.k8s.api.core.v1.EnvVar
import io.k8s.api.core.v1.EnvVarSource
import io.k8s.api.core.v1.ObjectFieldSelector
import io.k8s.api.core.v1.ResourceFieldSelector
import io.k8s.api.core.v1.ResourceRequirements
import io.k8s.apimachinery.pkg.api.resource.Quantity
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta

class K8SShutdown(k8s: Option[KubernetesClient[IO]]) extends ShutdownNode {
  def shutdownRunningNode(nodeName: RunningJobId): Unit = k8s match {
    case None =>
      scribe.error(
        s"Shut down $nodeName not happening because k8s client is empty. "
      )
    case Some(k8s) =>
      scribe.info(s"Shut down $nodeName")
      val spl = nodeName.value.split("/")
      val ns = spl(0)
      val podName = spl(1)
      val status = k8s.pods.namespace(ns).delete(name = podName).unsafeRunSync()
      scribe.info(s"Shutdown status of $podName pod: $status")
  }

  def shutdownPendingNode(nodeName: PendingJobId): Unit = k8s match {
    case None =>
      scribe.error(
        s"Shut down $nodeName not happening because k8s client is empty. "
      )
    case Some(k8s) =>
      scribe.info(s"Shut down $nodeName")
      val spl = nodeName.value.split("/")
      val ns = spl(0)
      val podName = spl(1)
      val status = k8s.pods.namespace(ns).delete(name = podName).unsafeRunSync()
      scribe.info(s"Shutdown status of $podName pod: $status")
  }

}

object KubernetesHelpers {
  def newName = Random.alphanumeric.take(128).mkString.toLowerCase
}

class K8SCreateNode(
    masterAddress: SimpleSocketAddress,
    codeAddress: CodeAddress,
    k8s: Option[KubernetesClient[IO]]
)(implicit config: TasksConfig, elasticSupport: ElasticSupportFqcn)
    extends CreateNode {

  def requestOneNewJobFromJobScheduler(
      requestSize: ResourceRequest
  ): Try[(PendingJobId, ResourceAvailable)] =
    k8s match {
      case None =>
        scribe.warn(
          "Spawning new node not happening because k8s client empty. "
        )
        scala.util.Failure(new RuntimeException("k8s client empty"))
      case Some(k8s) =>
        val userCPURequest =
          math.max(requestSize.cpu._2, config.kubernetesCpuMin)
        val userRamRequest =
          math.max(requestSize.memory, config.kubernetesRamMin)

        val kubeCPURequest = userCPURequest + config.kubernetesCpuExtra
        val kubeRamRequest = userRamRequest + config.kubernetesRamExtra
        val imageName = requestSize.image.getOrElse(config.kubernetesImageName)
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
          followerHostname = None,
          background = false,
          image = Some(imageName)
        )

        val command = Seq("/bin/bash", "-c", script)

        val podName = KubernetesHelpers.newName
        val jobName = config.kubernetesNamespace + "/" + podName

        

        val podSpecFromConfig: PodSpec = config.kubernetesPodSpec
          .map { jsonString =>
            val either = io.circe.parser.decode[PodSpec](jsonString)
            either.left.foreach(error => scribe.error(error))
            either.toOption.get
          }
          .getOrElse(PodSpec(containers = Nil))
        
        val containerFromConfig = podSpecFromConfig.containers.headOption.getOrElse(Container(""))

        val resource = Pod(
          metadata = Some(
            ObjectMeta(
              namespace = Some(config.kubernetesNamespace),
              name = Some(podName)
            )
          ),
          apiVersion = Some("v1"),
          spec = Some(
            podSpecFromConfig.copy(
              containers = List(
                containerFromConfig.copy(
                  image = Some(imageName),
                  command = Some(command),
                  name = "tasks-worker",
                  imagePullPolicy = Some(config.kubernetesImagePullPolicy),
                  env = Some(
                    containerFromConfig.env.getOrElse(Nil) ++
                    List(
                      EnvVar(
                        name = config.kubernetesHostNameOrIPEnvVar,
                        valueFrom = Some(
                          EnvVarSource(
                            fieldRef = Some(
                              ObjectFieldSelector(
                                fieldPath = "status.podIP"
                              )
                            )
                          )
                        )
                      ),
                      EnvVar(
                        name = config.kubernetesCpuLimitEnvVar,
                        valueFrom = Some(
                          EnvVarSource(
                            resourceFieldRef = Some(
                              ResourceFieldSelector(
                                containerName = Some("tasks-worker"),
                                resource = "limits.cpu"
                              )
                            )
                          )
                        )
                      ),
                      EnvVar(
                        name = config.kubernetesRamLimitEnvVar,
                        valueFrom = Some(
                          EnvVarSource(
                            resourceFieldRef = Some(
                              ResourceFieldSelector(
                                containerName = Some("tasks-worker"),
                                resource = "limits.memory"
                              )
                            )
                          )
                        )
                      ),
                      EnvVar(
                        name = config.kubernetesScratchLimitEnvVar,
                        valueFrom = Some(
                          EnvVarSource(
                            resourceFieldRef = Some(
                              ResourceFieldSelector(
                                containerName = Some("tasks-worker"),
                                resource = "limits.ephemeral-storage"
                              )
                            )
                          )
                        )
                      ),
                      EnvVar(
                        name = "TASKS_JOB_NAME",
                        value = Some(jobName)
                      )
                    )
                  ),
                  resources = Some(
                    ResourceRequirements(
                      requests = Some(
                        (Map(
                          "cpu" ->
                            Quantity(kubeCPURequest.toString),
                          "memory" -> new Quantity(s"${kubeRamRequest}M")
                        ) ++ (if (requestSize.scratch > 0)
                                Map(
                                  "ephemeral-storage" -> Quantity(
                                    s"${requestSize.scratch.toString}M"
                                  )
                                )
                              else Map.empty))
                      ),
                      limits = Some(
                        (if (requestSize.gpu > 0)
                           Map(
                             "nvidia.com/gpu" -> Quantity(
                               requestSize.gpu.toString
                             )
                           )
                         else Map.empty[String, Quantity])
                      )
                    )
                  )
                )
              ),
              restartPolicy = Some("Never")
            )
          )
        )
        scribe.info(resource.toString)

        val t = Try {
          val status = k8s.pods
            .namespace(config.kubernetesNamespace)
            .create(resource)
            .unsafeRunSync()
          scribe.info(s"Pod create status = $status of node $jobName")

          val available = ResourceAvailable(
            cpu = userCPURequest,
            memory = userRamRequest,
            scratch = requestSize.scratch,
            gpu = 0 until requestSize.gpu toList,
            image = Some(imageName)
          )

          (PendingJobId(jobName), available)
        }
        t

    }

}

class K8SCreateNodeFactory(k8s: Option[KubernetesClient[IO]])(implicit
    config: TasksConfig,
    fqcn: ElasticSupportFqcn
) extends CreateNodeFactory {
  def apply(master: SimpleSocketAddress, codeAddress: CodeAddress) =
    new K8SCreateNode(master, codeAddress, k8s)
}

object K8SGetNodeName extends GetNodeName {
  def getNodeName = System.getenv("TASKS_JOB_NAME")
}

class K8SElasticSupport extends ElasticSupportFromConfig {
  implicit val fqcn: ElasticSupportFqcn = ElasticSupportFqcn(
    "tasks.elastic.kubernetes.K8SElasticSupport"
  )
  def apply(implicit config: TasksConfig) = {
    implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

    val kubernetesClient =
      KubernetesClient[IO](
        KubeConfig.standard[IO].map(_.withDefaultAuthorizationCache(5.minutes))
      )
    kubernetesClient.attempt.map { k8s =>
      k8s.left.foreach { throwable =>
        scribe.error(
          throwable,
          "K8S client failed to create. Shutdown and nodefactory won't work"
        )
      }
      SimpleElasticSupport(
        fqcn = fqcn,
        hostConfig = Some(new K8SHostConfig()),
        reaperFactory = None,
        shutdown = new K8SShutdown(k8s.toOption),
        createNodeFactory = new K8SCreateNodeFactory(k8s.toOption),
        getNodeName = K8SGetNodeName
      )
    }
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
