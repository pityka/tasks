/*
 * The MIT License
 *
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
package tasks.elastic

import tasks.util._
import tasks.util.config._

import java.io.File

object Deployment {

  private[tasks] def pack(implicit config: TasksConfig): File = {
    val tmp = TempFile.createTempFile("package")
    val mainClassName = config.workerMainClass match {
      case ""    => None
      case other => Some(other)
    }
    selfpackage.write(tmp, mainClassName)
    tmp
  }

  def script(
      memory: Int,
      cpu: Int,
      scratch: Int,
      gpus: List[Int],
      masterAddress: SimpleSocketAddress,
      masterPrefix: String,
      download: Uri,
      followerHostname: Option[String],
      followerExternalHostname: Option[String],
      followerMayUseArbitraryPort: Boolean,
      followerNodeName: Option[String],
      background: Boolean,
      image: Option[String]
  )(implicit config: TasksConfig): String = {
    val packageFileFolder = config.workerWorkingDirectory
    val packageFileName = config.workerPackageName
    val downloadScript =
      s"cd $packageFileFolder && curl -m 60 $download > $packageFileName && chmod u+x $packageFileName"

    val hostnameString = followerHostname match {
      case None       => ""
      case Some(host) => s"-Dhosts.hostname=$host"
    }
    val externalHostnameString = followerExternalHostname match {
      case None       => ""
      case Some(host) => s"-Dhosts.hostnameExternal=$host"
    }
    val followerNodeNameString = followerNodeName match {
      case None        => ""
      case Some(value) => s"-Dtasks.elastic.nodename=$value"
    }

    val mayUseArbitraryPortString =
      s"-Dhosts.mayUseArbitraryPort=${followerMayUseArbitraryPort}"

    val gpuString =
      if (gpus.size > 0)
        s"-Dhosts.gpusAsCommaString=${gpus.map(_.toString).mkString(",")}"
      else ""

    val hostImageString =
      if (image.isDefined) s"-Dhosts.image=${image.get}" else ""

    val connectToProxyFileService =
      if (config.connectToProxyFileServiceOnMain)
        s"-Dtasks.fileservice.connectToProxy=true"
      else ""

    val edited =
      s"./$packageFileName -J-Xmx{RAM}M {EXTRA} -Dhosts.master={MASTER} -Dhosts.masterprefix=$masterPrefix  -Dhosts.app=false -Dtasks.fileservice.storageURI={STORAGE} -Dhosts.numCPU=$cpu -Dhosts.RAM=$memory -Dhosts.scratch=$scratch $gpuString $hostnameString $hostImageString $externalHostnameString  $mayUseArbitraryPortString $followerNodeNameString $connectToProxyFileService -Dtasks.disableRemoting=false"
        .replace(
          "{RAM}",
          math
            .max(1, (memory.toDouble * config.jvmMaxHeapFactor).toInt)
            .toString
        )
        .replace("{EXTRA}", config.additionalJavaCommandline)
        .replace(
          "{MASTER}",
          masterAddress.getHostName + ":" + masterAddress.getPort
        )
        .replace("{STORAGE}", config.storageURI.toString)

    val runPackage =
      if (background) s"""(nohup $edited 1> stdout 2> stderr & echo $$!;) """
      else s"$edited ;"

    s"""$downloadScript && $runPackage"""

  }
}
