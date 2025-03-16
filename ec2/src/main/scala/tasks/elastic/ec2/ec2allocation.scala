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

import scala.util._

import tasks.elastic._
import tasks.shared._
import tasks.util._
import tasks.util.config._

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

import scala.jdk.CollectionConverters._
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder
import com.amazonaws.services.ec2.AmazonEC2
import org.ekrich.config.Config
import org.ekrich.config.ConfigFactory
import cats.effect.IO
import cats.effect.kernel.Deferred
import cats.effect.ExitCode

class EC2Shutdown(ec2: AmazonEC2) extends ShutdownNode with ShutdownSelfNode {

  def shutdownRunningNode(nodeName: RunningJobId): IO[Unit] =
    IO.interruptible { EC2Operations.terminateInstance(ec2, nodeName.value) }
  def shutdownRunningNode(
      exitCode: Deferred[IO, ExitCode],
      nodeName: RunningJobId
  ): IO[Unit] =
    IO.interruptible { EC2Operations.terminateInstance(ec2, nodeName.value) }

  def shutdownPendingNode(nodeName: PendingJobId): IO[Unit] = IO.interruptible {
    val request = new CancelSpotInstanceRequestsRequest(
      List(nodeName.value).asJava
    )
    ec2.cancelSpotInstanceRequests(request)
  }

}

class EC2CreateNode(
    masterAddress: SimpleSocketAddress,
    masterPrefix: String,
    codeAddress: CodeAddress,
    ec2: AmazonEC2,
    ec2Config: EC2Config
) extends CreateNode {

  private def gzipBase64(str: String): String = {

    val out = new java.io.ByteArrayOutputStream();
    val gzip = new java.util.zip.GZIPOutputStream(out);
    gzip.write(str.getBytes());
    gzip.close();
    val bytes = out.toByteArray
    java.util.Base64.getEncoder.encodeToString(bytes)
  }

  def requestOneNewJobFromJobScheduler(
      requestSize: ResourceRequest
  )(implicit
      config: TasksConfig
  ): IO[Either[String, (PendingJobId, ResourceAvailable)]] =
    IO.interruptible {
      Try {
        val (requestid, instancetype) = requestSpotInstance(requestSize, config)
        val jobid = PendingJobId(requestid)
        val size = instancetype._2
        (jobid, size)
      }.toEither.left.map(_.getMessage)
    }

  override def initializeNode(node: Node): IO[Unit] = IO.interruptible {

    ec2.createTags(
      new CreateTagsRequest(
        List(node.name.value).asJava,
        ec2Config.instanceTags.map(t => new Tag(t._1, t._2)).asJava
      )
    )

  }

  override def convertRunningToPending(
      p: RunningJobId
  ): IO[Option[PendingJobId]] = IO.interruptible {
    val describeResult = ec2.describeSpotInstanceRequests();
    val spotInstanceRequests = describeResult.getSpotInstanceRequests();

    spotInstanceRequests.asScala
      .filter(_.getInstanceId == p.value)
      .headOption
      .map { x =>
        PendingJobId(x.getSpotInstanceRequestId)
      }

  }

  private def requestSpotInstance(
      requestSize: ResourceRequest,
      config: TasksConfig
  ) = {
    // size is ignored, instance specification is set in configuration
    val selectedInstanceType = EC2Operations
      .workerInstanceType(requestSize)(ec2Config)
      .getOrElse(
        throw new RuntimeException("No instance type could fullfill request")
      )

    // Initializes a Spot Instance Request
    val requestRequest = new RequestSpotInstancesRequest();

    if (ec2Config.spotPrice > 2.5)
      throw new RuntimeException("Spotprice too high:" + ec2Config.spotPrice)

    requestRequest.setSpotPrice(ec2Config.spotPrice.toString);
    requestRequest.setInstanceCount(1);
    requestRequest.setType(SpotInstanceType.OneTime)

    val launchSpecification = new LaunchSpecification();
    launchSpecification.setImageId(ec2Config.amiID);
    launchSpecification.setInstanceType(selectedInstanceType._1);
    launchSpecification.setKeyName(ec2Config.keyName)

    val blockDeviceMappingSDB = new BlockDeviceMapping();
    blockDeviceMappingSDB.setDeviceName("/dev/sdb");
    blockDeviceMappingSDB.setVirtualName("ephemeral0");
    val blockDeviceMappingSDC = new BlockDeviceMapping();
    blockDeviceMappingSDC.setDeviceName("/dev/sdc");
    blockDeviceMappingSDC.setVirtualName("ephemeral1");

    launchSpecification.setBlockDeviceMappings(
      List(blockDeviceMappingSDB, blockDeviceMappingSDC).asJava
    )

    ec2Config.iamRole.foreach { iamRole =>
      val iamprofile = new IamInstanceProfileSpecification()
      iamprofile.setName(iamRole)
      launchSpecification.setIamInstanceProfile(iamprofile)
    }

    ec2Config.placementGroup.foreach { string =>
      val placement = new SpotPlacement();
      placement.setGroupName(string);
      launchSpecification.setPlacement(placement);
    }

    val userdata = "#!/usr/bin/env bash\n" + Deployment.script(
      memory = selectedInstanceType._2.memory,
      cpu = selectedInstanceType._2.cpu,
      scratch = selectedInstanceType._2.scratch,
      gpus = selectedInstanceType._2.gpu,
      masterAddress = masterAddress,
      masterPrefix = masterPrefix,
      download = Uri(
        scheme = "http",
        hostname = codeAddress.address.getHostName,
        port = codeAddress.address.getPort,
        path = "/"
      ),
      followerHostname = None,
      followerExternalHostname = None,
      followerNodeName = None,
      followerMayUseArbitraryPort = true,
      background = true,
      image = None
    )(config)

    launchSpecification.setUserData(gzipBase64(userdata))

    val securitygroups =
      (ec2Config.securityGroup +: ec2Config.securityGroups).distinct
        .filter(_.size > 0)

    launchSpecification.setAllSecurityGroups(securitygroups.map { x =>
      val g = new GroupIdentifier
      g.setGroupId(x)
      g
    }.asJava)

    val subnetId = ec2Config.subnetId

    launchSpecification.setSubnetId(subnetId)

    // Add the launch specification.
    requestRequest.setLaunchSpecification(launchSpecification)

    // Call the RequestSpotInstance API.
    val requestResult = ec2.requestSpotInstances(requestRequest)

    (
      requestResult.getSpotInstanceRequests.asScala
        .map(_.getSpotInstanceRequestId)
        .head,
      selectedInstanceType
    )

  }

}

class EC2CreateNodeFactory(
    config: EC2Config,
    ec2: AmazonEC2
) extends CreateNodeFactory {
  def apply(
      master: SimpleSocketAddress,
      masterPrefix: String,
      codeAddress: CodeAddress
  ) =
    new EC2CreateNode(
      masterAddress = master,
      masterPrefix = masterPrefix,
      codeAddress = codeAddress,
      ec2 = ec2,
      ec2Config = config
    )
}

object EC2GetNodeName extends GetNodeName {
  def getNodeName(config: TasksConfig) =
    IO.interruptible(EC2Operations.readMetadata("instance-id").head)
}

class EC2Config(val raw: Config) extends ConfigValuesForHostConfiguration {
  val awsRegion: String = raw.getString("tasks.elastic.aws.region")

  def spotPrice: Double = raw.getDouble("tasks.elastic.aws.spotPrice")

  def amiID: String = raw.getString("tasks.elastic.aws.ami")

  def securityGroup: String = raw.getString("tasks.elastic.aws.securityGroup")

  def ec2InstanceTypes =
    raw.getConfigList("tasks.elastic.aws.instances").asScala.toList.map {
      conf =>
        val name = conf.getString("name")
        val cpu = conf.getInt("cpu")
        val ram = conf.getInt("ram")
        val gpu = conf.getInt("gpu")
        name -> ResourceAvailable(
          cpu,
          ram,
          Int.MaxValue,
          0 until gpu toList,
          None
        )
    }

  def securityGroups: List[String] =
    raw.getStringList("tasks.elastic.aws.securityGroups").asScala.toList

  def subnetId = raw.getString("tasks.elastic.aws.subnetId")

  def keyName = raw.getString("tasks.elastic.aws.keyName")

  def instanceTags =
    raw
      .getStringList("tasks.elastic.aws.tags")
      .asScala
      .grouped(2)
      .map(x => x(0) -> x(1))
      .toList

  val terminateMaster = raw.getBoolean("tasks.elastic.aws.terminateMaster")

  def iamRole = {
    val s = raw.getString("tasks.elastic.aws.iamRole")
    if (s == "" || s == "-") None
    else Some(s)
  }

  def placementGroup: Option[String] =
    raw.getString("tasks.elastic.aws.placementGroup") match {
      case x if x == "" => None
      case x            => Some(x)
    }
}

object EC2ElasticSupport {

  import cats.effect.IO
  def apply(config: Option[Config]) = {
    val ec2Config = new EC2Config(tasks.util.loadConfig(config))
    cats.effect.Resource.make {
      IO {

        implicit val ec2 =
          if (ec2Config.awsRegion.isEmpty) AmazonEC2ClientBuilder.defaultClient
          else
            AmazonEC2ClientBuilder.standard
              .withRegion(ec2Config.awsRegion)
              .build
        new ElasticSupport(
          hostConfig = Some(new EC2MasterSlave(ec2Config)),
          shutdownFromNodeRegistry = new EC2Shutdown(ec2),
          shutdownFromWorker = new EC2Shutdown(ec2),
          createNodeFactory = new EC2CreateNodeFactory(ec2Config, ec2),
          getNodeName = EC2GetNodeName
        )
      }
    } { release =>
      if (ec2Config.terminateMaster) cats.effect.IO {
        val ec2 =
          if (ec2Config.awsRegion.isEmpty) AmazonEC2ClientBuilder.defaultClient
          else
            AmazonEC2ClientBuilder.standard
              .withRegion(ec2Config.awsRegion)
              .build

        val nodename = EC2Operations.readMetadata("instance-id").head
        EC2Operations.terminateInstance(ec2, nodename)

      }
      else IO.unit
    }
  }
}
