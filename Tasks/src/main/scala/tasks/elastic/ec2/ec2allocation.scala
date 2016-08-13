/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
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
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification
import com.amazonaws.services.ec2.model.SpotInstanceType
import com.amazonaws.services.ec2.model.BlockDeviceMapping
import com.amazonaws.services.ec2.model.CancelSpotInstanceRequestsRequest
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.StorageClass
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.ec2.model.SpotPlacement
import com.amazonaws.AmazonServiceException
import java.net.URL

import collection.JavaConversions._
import java.io.{File, InputStream}

object EC2Helpers {

  val instanceTypes = List(
      "m3.medium" -> CPUMemoryAvailable(1, 3750),
      "c3.large" -> CPUMemoryAvailable(2, 3750),
      "m3.xlarge" -> CPUMemoryAvailable(4, 7500),
      "c3.xlarge" -> CPUMemoryAvailable(4, 7500),
      "r3.large" -> CPUMemoryAvailable(2, 15000),
      "m3.2xlarge" -> CPUMemoryAvailable(8, 15000),
      "c3.2xlarge" -> CPUMemoryAvailable(8, 15000),
      "r3.xlarge" -> CPUMemoryAvailable(4, 30000),
      "c3.4xlarge" -> CPUMemoryAvailable(16, 30000),
      "r3.2xlarge" -> CPUMemoryAvailable(8, 60000),
      "c3.8xlarge" -> CPUMemoryAvailable(32, 60000),
      "r3.4xlarge" -> CPUMemoryAvailable(16, 120000),
      "r3.8xlarge" -> CPUMemoryAvailable(32, 240000)
  )

}

object EC2Operations {

  def terminateInstance(ec2: AmazonEC2Client, instanceId: String): Unit = {
    retry(5) {
      val terminateRequest = new TerminateInstancesRequest(List(instanceId));
      ec2.terminateInstances(terminateRequest);
    }
  }

  def s3contains(bucketName: String, name: String): Boolean = {
    val s3Client = new AmazonS3Client()

    Try(s3Client.getObjectMetadata(bucketName, name)) match {
      case Success(_) => true
      case _ => false
    }
  }

  def downloadFile(bucketName: String, name: String): File = {
    retry(5) {
      val s3Client = new AmazonS3Client()
      val tm = new TransferManager(s3Client);
      val file = TempFile.createFileInTempFolderIfPossibleWithName(name)
      val download = tm.download(bucketName, name, file)
      download.waitForCompletion
      file
    }.get
  }

  def readMetadata(key: String): List[String] = {
    val source =
      scala.io.Source.fromURL("http://169.254.169.254/latest/meta-data/" + key)
    val list = source.getLines.toList
    source.close
    list
  }

}

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

  override def refreshPendingList: List[PendingJobId] = {
    val describeResult = ec2.describeSpotInstanceRequests();
    val spotInstanceRequests = describeResult.getSpotInstanceRequests();
    spotInstanceRequests
      .filter(x => x.getState == "open")
      .map(x => PendingJobId(x.getSpotInstanceRequestId))
      .toList
  }

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
    val selectedInstanceType = config.global.instanceType

    // Initializes a Spot Instance Request
    val requestRequest = new RequestSpotInstancesRequest();

    if (config.global.spotPrice > 2.5)
      throw new RuntimeException(
          "Spotprice too high:" + config.global.spotPrice)

    requestRequest.setSpotPrice(config.global.spotPrice.toString);
    requestRequest.setInstanceCount(1);
    requestRequest.setType(SpotInstanceType.OneTime)

    val launchSpecification = new LaunchSpecification();
    launchSpecification.setImageId(config.global.amiID);
    launchSpecification.setInstanceType(selectedInstanceType._1);
    launchSpecification.setKeyName(config.global.keyName)

    val blockDeviceMappingSDB = new BlockDeviceMapping();
    blockDeviceMappingSDB.setDeviceName("/dev/sdb");
    blockDeviceMappingSDB.setVirtualName("ephemeral0");
    val blockDeviceMappingSDC = new BlockDeviceMapping();
    blockDeviceMappingSDC.setDeviceName("/dev/sdc");
    blockDeviceMappingSDC.setVirtualName("ephemeral1");

    launchSpecification.setBlockDeviceMappings(
        List(blockDeviceMappingSDB, blockDeviceMappingSDC));

    config.global.iamRole.foreach { iamRole =>
      val iamprofile = new IamInstanceProfileSpecification()
      iamprofile.setName(iamRole)
      launchSpecification.setIamInstanceProfile(iamprofile)
    }

    config.global.placementGroup.foreach { string =>
      val placement = new SpotPlacement();
      placement.setGroupName(string);
      launchSpecification.setPlacement(placement);
    }

    val userdata = "#!/usr/bin/env bash\n" + Deployment.script(
          memory = selectedInstanceType._2.memory,
          gridEngine = tasks.EC2Grid,
          masterAddress = masterAddress,
          packageFile = config.global.tarball.get
      )

    launchSpecification.setUserData(gzipBase64(userdata))

    // Add the security group to the request.
    val securitygroups = (Set(config.global.securityGroup) & EC2Operations
          .readMetadata("security-groups")
          .toSet).toList
    launchSpecification.setSecurityGroups(securitygroups)

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

    val ackil = context.actorOf(
        Props(new EC2NodeKiller(ac, node))
          .withDispatcher("my-pinned-dispatcher"),
        "nodekiller" + node.name.value.replace("://", "___"))

  }

}

class EC2NodeKiller(
    val targetLauncherActor: ActorRef,
    val targetNode: Node
) extends NodeKillerImpl
    with EC2Shutdown
    with akka.actor.ActorLogging {
  val ec2 = new AmazonEC2Client()
  ec2.setEndpoint(config.global.endpoint)
}

class EC2NodeRegistry(
    val masterAddress: InetSocketAddress,
    val targetQueue: ActorRef,
    override val unmanagedResource: CPUMemoryAvailable
) extends EC2NodeRegistryImp
    with NodeCreatorImpl
    with SimpleDecideNewNode
    with EC2Shutdown
    with akka.actor.ActorLogging {
  val ec2 = new AmazonEC2Client()
  ec2.setEndpoint(config.global.endpoint)
}

trait EC2HostConfiguration extends HostConfiguration {

  private val myPort = chooseNetworkPort

  private val myhostname = EC2Operations.readMetadata("local-hostname").head

  lazy val myAddress = new InetSocketAddress(myhostname, myPort)

  private lazy val instancetype = EC2Helpers.instanceTypes
    .find(_._1 == EC2Operations.readMetadata("instance-type").head)
    .get

  lazy val availableMemory = instancetype._2.memory

  lazy val myCardinality = instancetype._2.cpu

}

object EC2MasterSlave
    extends MasterSlaveConfiguration
    with EC2HostConfiguration

class EC2SelfShutdown(val id: RunningJobId, val balancerActor: ActorRef)
    extends SelfShutdown
    with EC2Shutdown {
  val ec2 = new AmazonEC2Client()

}

class EC2Reaper(filesToSave: List[File],
                s3path: Option[(String, String)],
                terminateSelf: Boolean)
    extends Reaper
    with EC2Shutdown {

  val ec2 = new AmazonEC2Client()
  val s3Client = new AmazonS3Client();

  def allSoulsReaped(): Unit = {
    log.debug("All souls reaped. Calling system.shutdown.")

    if (s3path.isDefined) {
      import com.amazonaws.services.ec2.AmazonEC2Client
      import com.amazonaws.auth.InstanceProfileCredentialsProvider

      val tm = new TransferManager(s3Client);
      try {
        // Asynchronous call.
        filesToSave.foreach { f =>
          val putrequest =
            new PutObjectRequest(s3path.get._1,
                                 s3path.get._2 + "/" + f.getName,
                                 f)

          { // set server side encryption
            val objectMetadata = new ObjectMetadata();
            objectMetadata.setSSEAlgorithm(
                ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
            putrequest.setMetadata(objectMetadata);
          }

          val upload = tm.upload(putrequest);
          upload.waitForCompletion
        }
      } finally {
        tm.shutdownNow
      }
    }
    if (terminateSelf) {
      val nodename = EC2Operations.readMetadata("instance-id").head
      EC2Operations.terminateInstance(ec2, nodename)
    }
    context.system.shutdown
  }
}
