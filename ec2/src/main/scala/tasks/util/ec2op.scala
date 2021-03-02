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

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

import scala.jdk.CollectionConverters._

import tasks.shared._
import tasks.util.config._

object EC2Operations {

  val scratch = Int.MaxValue

  val instanceTypes = List(
    "m3.medium" -> ResourceAvailable(1, 3750, scratch, Nil),
    "c3.large" -> ResourceAvailable(2, 3750, scratch, Nil),
    "m3.xlarge" -> ResourceAvailable(4, 7500, scratch, Nil),
    "c3.xlarge" -> ResourceAvailable(4, 7500, scratch, Nil),
    "r3.large" -> ResourceAvailable(2, 15000, scratch, Nil),
    "m3.2xlarge" -> ResourceAvailable(8, 15000, scratch, Nil),
    "c3.2xlarge" -> ResourceAvailable(8, 15000, scratch, Nil),
    "r3.xlarge" -> ResourceAvailable(4, 30000, scratch, Nil),
    "c3.4xlarge" -> ResourceAvailable(16, 30000, scratch, Nil),
    "r3.2xlarge" -> ResourceAvailable(8, 60000, scratch, Nil),
    "c3.8xlarge" -> ResourceAvailable(32, 60000, scratch, Nil),
    "r3.4xlarge" -> ResourceAvailable(16, 120000, scratch, Nil),
    "r3.8xlarge" -> ResourceAvailable(32, 240000, scratch, Nil),
    "p2.xlarge" -> ResourceAvailable(4, 61000, scratch, List(0)),
    "p2.8xlarge" -> ResourceAvailable(32, 488000, scratch, List(0 until 8: _*))
  )

  def currentInstanceType =
    instanceTypes
      .find(_._1 == readMetadata("instance-type").head)
      .getOrElse(instanceTypes.head)

  def slaveInstanceType(implicit config: TasksConfig) =
    instanceTypes
      .find(_._1 == config.slaveInstanceType)
      .getOrElse(instanceTypes.head)

  def terminateInstance(ec2: AmazonEC2Client, instanceId: String): Unit = {
    retry(5) {
      val terminateRequest =
        new TerminateInstancesRequest(List(instanceId).asJava);
      ec2.terminateInstances(terminateRequest);
    }
  }

  def readMetadata(key: String): List[String] = {
    val source =
      scala.io.Source.fromURL("http://169.254.169.254/latest/meta-data/" + key)
    val list = source.getLines().toList
    source.close
    list
  }

}
