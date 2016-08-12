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

  def terminateInstance(ec2: AmazonEC2Client, instanceId: String) {
    retry(5) {
      val terminateRequest = new TerminateInstancesRequest(List(instanceId));
      ec2.terminateInstances(terminateRequest);
    }
  }

  def S3contains(bucketName: String, name: String): Boolean = {
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
    val selectedInstanceType = config.instanceType

    // Initializes a Spot Instance Request
    val requestRequest = new RequestSpotInstancesRequest();

    if (config.spotPrice > 2.4)
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

    val extraFiles = config.extraFilesFromS3
      .grouped(3)
      .map { l =>
        s"python getFile.py ${l(0)} ${l(1)} ${l(2)} && chmod u+x ${l(2)} "
      }
      .mkString("\n")

    val launchCommand = if (config.launchWithDocker.isEmpty) {
      val javacommand =
        s"nohup java -Xmx${(selectedInstanceType._2.memory.toDouble * config.jvmMaxHeapFactor).toInt}M -Dhosts.gridengine=EC2 ${config.additionalJavaCommandline} -Dconfig.file=rendered.conf -Dhosts.master=${masterAddress.getHostName + ":" + masterAddress.getPort} -jar pipeline.jar > pipelinestdout &"

      s"python getFile.py ${config.jarBucket} ${config.jarObject} pipeline.jar" ::
        s"""cat << EOF > rendered.conf
${config.asString}
EOF""" :: javacommand :: Nil
    } else {
      s"docker run --rm -v /media/ephemeral0/tmp/scatch:/scratch ${config.launchWithDocker.get._1}/${config.launchWithDocker.get._2} -J-Dhosts.master=${masterAddress.getHostName + ":" + masterAddress.getPort} 1> stdout 2> stderr &" :: Nil
    }

    val userdata =
      "#!/bin/bash" ::
        """
# Download myFile
cat << EOF > getFile.py
import boto;import sys;boto.connect_s3().get_bucket(sys.argv[1]).get_key(sys.argv[2]).get_contents_to_filename(sys.argv[3])
EOF

# Set up LVM on all the ephemeral disks
EPHEMERALS=`curl http://169.254.169.254/latest/meta-data/block-device-mapping/ | grep ephemeral`
DEVICES=$(for i in $EPHEMERALS
 do
 DEVICE=`curl http://169.254.169.254/latest/meta-data/block-device-mapping/$i/`
 umount /dev/$DEVICE
 echo /dev/$DEVICE
done)
pvcreate $DEVICES
vgcreate vg $DEVICES
TOTALPE=`vgdisplay vg | grep "Total PE" | awk '{print $3;}'`
lvcreate -l $TOTALPE vg
mkfs -t ext4 /dev/vg/lvol0
mount /dev/vg/lvol0 /media/ephemeral0/
mkdir -m 1777 /media/ephemeral0/tmp/
mkdir -m 1777 /media/ephemeral0/tmp/scatch
mount --bind /media/ephemeral0/tmp/ /tmp
""" ::
          extraFiles ::
            config.extraStartupscript ::
              """export PATH=./:$PATH""" ::
                launchCommand ::
                  Nil

    launchSpecification.setUserData(gzipBase64(userdata.mkString("\n")))

    // Add the security group to the request.
    val securitygroups = (Set(config.securityGroup) & EC2Operations
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
    val ac = node.launcherActor //.revive

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
  ec2.setEndpoint(config.endpoint)
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
  ec2.setEndpoint(config.endpoint)
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
                bucketName: String,
                folderPrefix: String)
    extends Reaper
    with EC2Shutdown {

  val ec2 = new AmazonEC2Client()
  val s3Client = new AmazonS3Client();

  def allSoulsReaped(): Unit = {
    log.debug("All souls reaped. Calling system.shutdown.")
    context.system.shutdown()
    import com.amazonaws.services.ec2.AmazonEC2Client
    import com.amazonaws.auth.InstanceProfileCredentialsProvider

    val tm = new TransferManager(s3Client);
    // Asynchronous call.
    filesToSave.foreach { f =>
      val putrequest =
        new PutObjectRequest(bucketName, folderPrefix + "/" + f.getName, f)

      putrequest.setCannedAcl(CannedAccessControlList.Private)

      { // set server side encryption
        val objectMetadata = new ObjectMetadata();
        objectMetadata.setSSEAlgorithm(
            ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        putrequest.setMetadata(objectMetadata);
      }

      val upload = tm.upload(putrequest);
      upload.waitForCompletion
    }

    val nodename = EC2Operations.readMetadata("instance-id").head
    EC2Operations.terminateInstance(ec2, nodename)
  }
}

class S3Updater(filesToSave: List[File],
                bucketName: String,
                folderPrefix: String)
    extends Actor
    with akka.actor.ActorLogging {
  case object Tick

  override def preStart {
    log.debug("S3Updater start (" + filesToSave + ")")

    import context.dispatcher

    timer = context.system.scheduler.schedule(
        initialDelay = 0 seconds,
        interval = config.s3UpdateInterval,
        receiver = self,
        message = Tick
    )
  }

  val s3Client = new AmazonS3Client();
  val tm = new TransferManager(s3Client);

  private var timer: Cancellable = null

  override def postStop {
    timer.cancel
    log.info("HeartBeatActor stopped.")
  }

  def upload {
    Try {
      filesToSave.foreach { f =>
        val putrequest =
          new PutObjectRequest(bucketName, folderPrefix + "/" + f.getName, f)

        putrequest.setCannedAcl(CannedAccessControlList.Private)

        { // set server side encryption
          val objectMetadata = new ObjectMetadata();
          objectMetadata.setSSEAlgorithm(
              ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
          putrequest.setMetadata(objectMetadata);
        }

        val upload = tm.upload(putrequest);
        upload.waitForCompletion
        log.debug("Uploaded " + f.toString)
      }
    }
  }

  def receive = {
    case Tick => upload
  }

}
