package tasks.elastic

import tasks._
import tasks.util._
import tasks.util.eq._

import java.net._

object Deployment {

  def script(
      memory: Int,
      gridEngine: GridEngine,
      masterAddress: InetSocketAddress,
      packageFile: URL
  ): String = {
    val download =
      if (packageFile.getProtocol === "s3")
        s"aws s3 cp $packageFile package"
      else s"curl $packageFile > package"

    val tar = "tar xf package"

    val javacommand =
      s"""nohup bin/entrypoint -J-Xmx${(memory.toDouble * config.global.jvmMaxHeapFactor).toInt}M -Dhosts.gridengine=$gridEngine ${config.global.additionalJavaCommandline}  -Dhosts.master=${masterAddress.getHostName + ":" + masterAddress.getPort} -Dtasks.elastic.enabled=true 1> stdout 2>stderr &
    """

    "#!/usr/bin/env bash\n" + download + "&&" + tar + "&&" + javacommand

  }
}
