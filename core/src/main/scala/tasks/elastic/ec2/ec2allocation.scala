/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Modified work, Copyright (c) 2016 Istvan Bartha


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

package tasks.elastic.ec2

import akka.actor.{Actor, ActorRef, Props}
import java.net.InetSocketAddress
import akka.event.LoggingAdapter
import scala.util._

import tasks.elastic._
import tasks.shared._
import tasks.util._
import tasks.util.config._

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CancelSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.LaunchSpecification;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.GroupIdentifier
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification
import com.amazonaws.services.ec2.model.SpotInstanceType
import com.amazonaws.services.ec2.model.BlockDeviceMapping
import com.amazonaws.services.ec2.model.CancelSpotInstanceRequestsRequest
import com.amazonaws.services.ec2.model.SpotPlacement
import com.amazonaws.services.ec2.model.CreateTagsRequest

import scala.collection.JavaConverters._

trait EC2Shutdown extends ShutdownNode {

  def log: LoggingAdapter

  val ec2: AmazonEC2Client

  def shutdownRunningNode(nodeName: RunningJobId): Unit =
    EC2Operations.terminateInstance(ec2, nodeName.value)

  def shutdownPendingNode(nodeName: PendingJobId): Unit = {
    val request = new CancelSpotInstanceRequestsRequest(
      List(nodeName.value).asJava)
    ec2.cancelSpotInstanceRequests(request)
  }

}

trait EC2NodeRegistryImp extends Actor with GridJobRegistry {

  implicit def config: tasks.util.config.TasksConfig

  var counter = 0

  private def gzipBase64(str: String): String = {

    val out = new java.io.ByteArrayOutputStream();
    val gzip = new java.util.zip.GZIPOutputStream(out);
    gzip.write(str.getBytes());
    gzip.close();
    val bytes = out.toByteArray
    javax.xml.bind.DatatypeConverter.printBase64Binary(bytes)
  }

  def masterAddress: InetSocketAddress

  def codeAddress: CodeAddress

  def ec2: AmazonEC2Client

  override def convertRunningToPending(
      p: RunningJobId): Option[PendingJobId] = {
    val describeResult = ec2.describeSpotInstanceRequests();
    val spotInstanceRequests = describeResult.getSpotInstanceRequests();

    spotInstanceRequests.asScala
      .filter(_.getInstanceId == p.value)
      .headOption
      .map { x =>
        PendingJobId(x.getSpotInstanceRequestId)
      }

  }

  private def requestSpotInstance = {
    // size is ignored, instance specification is set in configuration
    val selectedInstanceType = EC2Operations.slaveInstanceType

    // Initializes a Spot Instance Request
    val requestRequest = new RequestSpotInstancesRequest();

    if (config.spotPrice > 2.5)
      throw new RuntimeException("Spotprice too high:" + config.spotPrice)

    requestRequest.setSpotPrice(config.spotPrice.toString);
    requestRequest.setInstanceCount(1);
    requestRequest.setType(SpotInstanceType.OneTime)

    val launchSpecification = new LaunchSpecification();
    launchSpecification.setImageId(config.amiID);
    launchSpecification.setInstanceType(selectedInstanceType._1);
    launchSpecification.setKeyName(config.keyName)

    val blockDeviceMappingSDB = new BlockDeviceMapping();
    blockDeviceMappingSDB.setDeviceName("/dev/sdb");
    blockDeviceMappingSDB.setVirtualName("ephemeral0");
    val blockDeviceMappingSDC = new BlockDeviceMapping();
    blockDeviceMappingSDC.setDeviceName("/dev/sdc");
    blockDeviceMappingSDC.setVirtualName("ephemeral1");

    launchSpecification.setBlockDeviceMappings(
      List(blockDeviceMappingSDB, blockDeviceMappingSDC).asJava)

    config.iamRole.foreach { iamRole =>
      val iamprofile = new IamInstanceProfileSpecification()
      iamprofile.setName(iamRole)
      launchSpecification.setIamInstanceProfile(iamprofile)
    }

    config.placementGroup.foreach { string =>
      val placement = new SpotPlacement();
      placement.setGroupName(string);
      launchSpecification.setPlacement(placement);
    }

    val userdata = "#!/usr/bin/env bash\n" + Deployment.script(
      memory = selectedInstanceType._2.memory,
      gridEngine = EC2Grid,
      masterAddress = masterAddress,
      download = new java.net.URL("http",
                                  codeAddress.address.getHostName,
                                  codeAddress.address.getPort,
                                  "/")
    )

    launchSpecification.setUserData(gzipBase64(userdata))

    val securitygroups =
      (config.securityGroup +: config.securityGroups).distinct
        .filter(_.size > 0)

    launchSpecification.setAllSecurityGroups(securitygroups.map { x =>
      val g = new GroupIdentifier
      g.setGroupId(x)
      g
    }.asJava)

    val subnetId = config.subnetId

    launchSpecification.setSubnetId(subnetId)

    // Add the launch specification.
    requestRequest.setLaunchSpecification(launchSpecification)

    // Call the RequestSpotInstance API.
    val requestResult = ec2.requestSpotInstances(requestRequest)

    (requestResult.getSpotInstanceRequests.asScala
       .map(_.getSpotInstanceRequestId)
       .head,
     selectedInstanceType)

  }

  def requestOneNewJobFromGridScheduler(request: CPUMemoryRequest)
    : Try[Tuple2[PendingJobId, CPUMemoryAvailable]] = Try {
    val (requestid, instancetype) = requestSpotInstance
    val jobid = PendingJobId(requestid)
    val size = CPUMemoryAvailable(
      cpu = instancetype._2.cpu,
      memory = instancetype._2.memory
    )
    (jobid, size)
  }

  def initializeNode(node: Node): Unit = {
    val ac = node.launcherActor

    ec2.createTags(
      new CreateTagsRequest(
        List(node.name.value).asJava,
        config.instanceTags.map(t => new Tag(t._1, t._2)).asJava))

    context.actorOf(Props(new EC2NodeKiller(ac, node))
                      .withDispatcher("my-pinned-dispatcher"),
                    "nodekiller" + node.name.value.replace("://", "___"))

  }

}

class EC2NodeKiller(
    val targetLauncherActor: ActorRef,
    val targetNode: Node
)(implicit val config: TasksConfig)
    extends NodeKillerImpl
    with EC2Shutdown
    with akka.actor.ActorLogging {
  val ec2 = new AmazonEC2Client()
  ec2.setEndpoint(config.endpoint)
}

class EC2NodeRegistry(
    val masterAddress: InetSocketAddress,
    val targetQueue: ActorRef,
    override val unmanagedResource: CPUMemoryAvailable,
    val codeAddress: CodeAddress
)(implicit val config: TasksConfig)
    extends EC2NodeRegistryImp
    with NodeCreatorImpl
    with SimpleDecideNewNode
    with EC2Shutdown
    with akka.actor.ActorLogging {
  val ec2 = new AmazonEC2Client()
  ec2.setEndpoint(config.endpoint)
  def codeVersion = codeAddress.codeVersion
}

class EC2SelfShutdown(val id: RunningJobId, val balancerActor: ActorRef)(
    implicit val config: TasksConfig)
    extends SelfShutdown
    with EC2Shutdown {
  val ec2 = new AmazonEC2Client()
  ec2.setEndpoint(config.endpoint)
}

class EC2Reaper(terminateSelf: Boolean)(implicit val config: TasksConfig)
    extends Reaper
    with EC2Shutdown {

  val ec2 = new AmazonEC2Client()
  ec2.setEndpoint(config.endpoint)

  def allSoulsReaped(): Unit = {
    log.debug("All souls reaped. Calling system.shutdown.")
    if (terminateSelf) {
      val nodename = EC2Operations.readMetadata("instance-id").head
      EC2Operations.terminateInstance(ec2, nodename)
    }
    context.system.terminate()
  }
}

object EC2Grid extends ElasticSupport[EC2NodeRegistry, EC2SelfShutdown] {

  def apply(masterAddress: InetSocketAddress,
            balancerActor: ActorRef,
            resource: CPUMemoryAvailable,
            codeAddress: Option[CodeAddress])(implicit config: TasksConfig) =
    new Inner {

      def getNodeName = EC2Operations.readMetadata("instance-id").head
      def createRegistry =
        codeAddress.map(
          codeAddress =>
            new EC2NodeRegistry(masterAddress,
                                balancerActor,
                                resource,
                                codeAddress))
      def createSelfShutdown =
        new EC2SelfShutdown(RunningJobId(getNodeName), balancerActor)
    }

  override def toString = "EC2"

}
