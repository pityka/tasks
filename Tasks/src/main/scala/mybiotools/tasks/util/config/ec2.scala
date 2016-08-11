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

package tasks.util.config

import tasks.elastic.ec2._
import scala.collection.JavaConversions._

trait EC2Settings {

  val endpoint: String = global.getString("tasks.elastic.aws.endpoint")

  val spotPrice: Double = global.getDouble("tasks.elastic.aws.spotPrice")

  val amiID: String = global.getString("tasks.elastic.aws.ami")

  val instanceType = EC2Helpers.instanceTypes
    .find(_._1 == global.getString("tasks.elastic.aws.instanceType"))
    .get

  val securityGroup: String =
    global.getString("tasks.elastic.aws.securityGroup")

  val jarBucket: String = global.getString("tasks.elastic.aws.jarBucket")

  val jarObject: String = global.getString("tasks.elastic.aws.jarObject")

  val keyName = global.getString("tasks.elastic.aws.keyName")

  val extraFilesFromS3: List[String] =
    global.getStringList("tasks.elastic.aws.extraFilesFromS3").toList

  val extraStartupscript: String =
    global.getString("tasks.elastic.aws.extraStartupScript")

  val additionalJavaCommandline =
    global.getString("tasks.elastic.aws.extraJavaCommandline")

  val iamRole = {
    val s = global.getString("tasks.elastic.aws.iamRole")
    if (s == "" || s == "-") None
    else Some(s)
  }

  val s3UpdateInterval: FD =
    global.getDuration("tasks.elastic.aws.uploadInterval")

  val placementGroup: Option[String] =
    global.getString("tasks.elastic.aws.placementGroup") match {
      case x if x == "" => None
      case x => Some(x)
    }

  val jvmMaxHeapFactor = global.getDouble("tasks.elastic.aws.jvmMaxHeapFactor")

}
