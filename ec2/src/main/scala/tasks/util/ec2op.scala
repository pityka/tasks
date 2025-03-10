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

import com.amazonaws.services.ec2.AmazonEC2
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

import scala.jdk.CollectionConverters._

import tasks.shared._
import tasks.util.config.TasksConfig
import tasks.elastic.ec2.EC2Config

object EC2Operations {

  val scratch = Int.MaxValue

  def instanceTypes(implicit config: EC2Config) = config.ec2InstanceTypes

  def currentInstanceType(implicit config: EC2Config) =
    instanceTypes
      .find(_._1 == readMetadata("instance-type").head)
      .getOrElse(instanceTypes.head)

  def workerInstanceType(
      requestSize: ResourceRequest
  )(implicit config: EC2Config) =
    instanceTypes
      .find { case (_, instancetype) =>
        instancetype.cpu >= requestSize.cpu._1 && instancetype.memory >= requestSize.memory && instancetype.scratch >= requestSize.scratch && instancetype.gpu.size >= requestSize.gpu
      }

  def terminateInstance(ec2: AmazonEC2, instanceId: String): Unit = {
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
