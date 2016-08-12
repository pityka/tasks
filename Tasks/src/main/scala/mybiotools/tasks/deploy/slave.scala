package tasks.deploy

import tasks._
import tasks.util._

import java.net._

object Deployment {

  def script(
      memory: Int,
      gridEngine: GridEngine,
      masterAddress: InetSocketAddress,
      tarball: URL
  ): String = {
    val download =
      if (tarball.getProtocol == "s3")
        s"aws s3 cp $tarball package"
      else s"curl $tarball > package"

    val tar = "tar xf package"

    val javacommand = s"""
    nohup bin/entrypoint -J-Xmx${(memory.toDouble * config.jvmMaxHeapFactor).toInt}M -Dhosts.gridengine=$gridEngine ${config.additionalJavaCommandline}  -Dhosts.master=${masterAddress.getHostName + ":" + masterAddress.getPort} -Dtasks.elastic.enabled=true 1> stdout 2>stderr &
    """

    "#!/usr/bin/env bash\n" + download + "&&" + tar + "&&" + javacommand

  }
}
