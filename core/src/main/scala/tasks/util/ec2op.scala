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

package tasks.util

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

import com.amazonaws.services.ec2.model.SpotPlacement
import com.amazonaws.AmazonServiceException
import java.net.URL

import collection.JavaConversions._
import scala.util._
import java.io._

import tasks.shared._

object EC2Operations {

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

  def currentInstanceType =
    instanceTypes.find(_._1 == readMetadata("instance-type").head).get

  val slaveInstanceType =
    instanceTypes.find(_._1 == config.global.slaveInstanceType).get

  def terminateInstance(ec2: AmazonEC2Client, instanceId: String): Unit = {
    retry(5) {
      val terminateRequest = new TerminateInstancesRequest(List(instanceId));
      ec2.terminateInstances(terminateRequest);
    }
  }

  def readMetadata(key: String): List[String] = {
    val source =
      scala.io.Source.fromURL("http://169.254.169.254/latest/meta-data/" + key)
    val list = source.getLines.toList
    source.close
    list
  }

}
