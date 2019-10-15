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

import java.net.InetSocketAddress
import scala.util._

import tasks.elastic._
import tasks.shared._
import tasks.util.config._
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.api.model.{EnvVar}
import io.fabric8.kubernetes.client.DefaultKubernetesClient
import scala.collection.JavaConverters._
import com.typesafe.scalalogging.StrictLogging

class K8SShutdown(k8s: KubernetesClient)
    extends ShutdownNode
    with StrictLogging {
  def shutdownRunningNode(nodeName: RunningJobId): Unit = {
    logger.info(s"Shut down $nodeName")
    k8s.pods.withName(nodeName.value).delete
  }

  def shutdownPendingNode(nodeName: PendingJobId): Unit = {
    logger.info(s"Shut down $nodeName")
    k8s.pods.withName(nodeName.value).delete
  }

}

object KubernetesHelpers {
  def newName = Random.alphanumeric.take(170).mkString.toLowerCase
}

class K8SCreateNode(
    masterAddress: InetSocketAddress,
    codeAddress: CodeAddress,
    k8s: KubernetesClient
)(implicit config: TasksConfig, elasticSupport: ElasticSupportFqcn)
    extends CreateNode {

  def requestOneNewJobFromJobScheduler(
      requestSize: ResourceRequest
  ): Try[(PendingJobId, ResourceAvailable)] = {
    val script = Deployment.script(
      memory = requestSize.memory,
      cpu = requestSize.cpu._2,
      scratch = requestSize.scratch,
      elasticSupport = elasticSupport,
      masterAddress = masterAddress,
      download = new java.net.URL(
        "http",
        codeAddress.address.getHostName,
        codeAddress.address.getPort,
        "/"
      ),
      slaveHostname = None,
      background = false
    )

    val command = Seq("/bin/bash", "-c", script)

    val name = KubernetesHelpers.newName

    val imageName = config.kubernetesImageName

    Try {

      k8s.pods.createNew.withNewMetadata
        .withName(name)
        .endMetadata
        .withNewSpec
        .addNewContainer
        .withImage(imageName)
        .withCommand(command.asJava)
        .withName("tasks")
        .withImagePullPolicy(config.kubernetesImagePullPolicy)
        .withEnv(new EnvVar("TASKS_JOB_NAME", name, null))
        .endContainer
        .withRestartPolicy("Never")
        .endSpec
        .done

      val available = requestSize.toAvailable

      (PendingJobId(name), available)
    }

  }

}

class K8SCreateNodeFactory(k8s: KubernetesClient)(
    implicit config: TasksConfig,
    fqcn: ElasticSupportFqcn
) extends CreateNodeFactory {
  def apply(master: InetSocketAddress, codeAddress: CodeAddress) =
    new K8SCreateNode(master, codeAddress, k8s)
}

object K8SGetNodeName extends GetNodeName {
  def getNodeName = System.getenv("TASKS_JOB_NAME")
}

object K8SElasticSupport extends ElasticSupportFromConfig {
  implicit val fqcn = ElasticSupportFqcn(
    "tasks.elastic.kubernetes.K8SElasticSupport"
  )
  def apply(implicit config: TasksConfig) = {
    val k8s =
      (new DefaultKubernetesClient).inNamespace(config.kubernetesNamespace)
    SimpleElasticSupport(
      fqcn = fqcn,
      hostConfig = None,
      reaperFactory = None,
      shutdown = new K8SShutdown(k8s),
      createNodeFactory = new K8SCreateNodeFactory(k8s),
      getNodeName = K8SGetNodeName
    )
  }
}
