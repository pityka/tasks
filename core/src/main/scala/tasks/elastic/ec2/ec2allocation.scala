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

import akka.actor.{Actor, PoisonPill, ActorRef, Props, Cancellable}
import scala.concurrent.duration._
import java.util.concurrent.{TimeUnit, ScheduledFuture}
import java.net.InetSocketAddress
import akka.actor.Actor._
import akka.event.LoggingAdapter
import scala.util._

import tasks.elastic._
import tasks.deploy._
import tasks.shared._
import tasks.util._
import tasks.util.config._
import tasks.util.eq._
import tasks.fileservice._

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CancelSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsResult;
import com.amazonaws.services.ec2.model.LaunchSpecification;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.RequestSpotInstancesResult;
import com.amazonaws.services.ec2.model.SpotInstanceRequest;
import com.amazonaws.services.ec2.model.GroupIdentifier
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification
import com.amazonaws.services.ec2.model.SpotInstanceType
import com.amazonaws.services.ec2.model.BlockDeviceMapping
import com.amazonaws.services.ec2.model.CancelSpotInstanceRequestsRequest
import com.amazonaws.services.ec2.model.SpotPlacement
import com.amazonaws.services.ec2.model.CreateTagsRequest
import com.amazonaws.AmazonServiceException
import java.net.URL

import collection.JavaConversions._
import java.io.{File, InputStream}

trait EC2Shutdown extends ShutdownNode {

  def log: LoggingAdapter

  val ec2: AmazonEC2Client

  def shutdownRunningNode(nodeName: RunningJobId): Unit =
    EC2Operations.terminateInstance(ec2, nodeName.value)

  def shutdownPendingNode(nodeName: PendingJobId): Unit = {
    val request = new CancelSpotInstanceRequestsRequest(List(nodeName.value))
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

  val masterAddress: InetSocketAddress

  val ec2: AmazonEC2Client

  override def convertRunningToPending(p: RunningJobId): Option[PendingJobId] = {
    val describeResult = ec2.describeSpotInstanceRequests();
    val spotInstanceRequests = describeResult.getSpotInstanceRequests();

    spotInstanceRequests.filter(_.getInstanceId == p.value).headOption.map {
      x =>
        PendingJobId(x.getSpotInstanceRequestId)
    }

  }

  private def requestSpotInstance(size: CPUMemoryRequest) = {
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
      List(blockDeviceMappingSDB, blockDeviceMappingSDC));

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
                                  masterAddress.getHostName,
                                  masterAddress.getPort + 1,
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
    })

    val subnetId = config.subnetId

    launchSpecification.setSubnetId(subnetId)

    // Add the launch specification.
    requestRequest.setLaunchSpecification(launchSpecification)

    // Call the RequestSpotInstance API.
    val requestResult = ec2.requestSpotInstances(requestRequest)

    (requestResult.getSpotInstanceRequests
       .map(_.getSpotInstanceRequestId)
       .head,
     selectedInstanceType)

  }

  def requestOneNewJobFromGridScheduler(request: CPUMemoryRequest)
    : Try[Tuple2[PendingJobId, CPUMemoryAvailable]] = Try {
    val (requestid, instancetype) = requestSpotInstance(request)
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
      new CreateTagsRequest(List(node.name.value),
                            config.instanceTags.map(t => new Tag(t._1, t._2))))

    val ackil = context.actorOf(
      Props(new EC2NodeKiller(ac, node))
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
    override val unmanagedResource: CPUMemoryAvailable
)(implicit val config: TasksConfig)
    extends EC2NodeRegistryImp
    with NodeCreatorImpl
    with SimpleDecideNewNode
    with EC2Shutdown
    with akka.actor.ActorLogging {
  val ec2 = new AmazonEC2Client()
  ec2.setEndpoint(config.endpoint)
}

class EC2SelfShutdown(val id: RunningJobId, val balancerActor: ActorRef)
  (implicit val config: TasksConfig)
    extends SelfShutdown
    with EC2Shutdown {
  val ec2 = new AmazonEC2Client()
  ec2.setEndpoint(config.endpoint)
}

class EC2Reaper(terminateSelf: Boolean)(implicit val config: TasksConfig) extends Reaper with EC2Shutdown {

  val ec2 = new AmazonEC2Client()
  ec2.setEndpoint(config.endpoint)

  def allSoulsReaped(): Unit = ()
  //   {
  //   log.debug("All souls reaped. Calling system.shutdown.")
  //
  //   if (s3path.isDefined) {
  //     import com.amazonaws.services.ec2.AmazonEC2Client
  //     import com.amazonaws.auth.InstanceProfileCredentialsProvider
  //
  //     val tm = new TransferManager(s3Client);
  //     try {
  //       // Asynchronous call.
  //       filesToSave.foreach { f =>
  //         val putrequest =
  //           new PutObjectRequest(s3path.get._1,
  //                                s3path.get._2 + "/" + f.getName,
  //                                f)
  //
  //         { // set server side encryption
  //           val objectMetadata = new ObjectMetadata();
  //           objectMetadata.setSSEAlgorithm(
  //               ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
  //           putrequest.setMetadata(objectMetadata);
  //         }
  //
  //         val upload = tm.upload(putrequest);
  //         upload.waitForCompletion
  //       }
  //     } finally {
  //       tm.shutdownNow
  //     }
  //   }
  //   if (terminateSelf) {
  //     val nodename = EC2Operations.readMetadata("instance-id").head
  //     EC2Operations.terminateInstance(ec2, nodename)
  //   }
  //   context.system.shutdown
  // }
}

object EC2Grid extends ElasticSupport[EC2NodeRegistry, EC2SelfShutdown] {

  def apply(master: InetSocketAddress,
            balancerActor: ActorRef,
            resource: CPUMemoryAvailable)(implicit config: TasksConfig) =
    new Inner {
      def getNodeName = EC2Operations.readMetadata("instance-id").head
      def createRegistry = new EC2NodeRegistry(master, balancerActor, resource)
      def createSelfShutdown =
        new EC2SelfShutdown(RunningJobId(getNodeName), balancerActor)
    }

  override def toString = "EC2"

}
