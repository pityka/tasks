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

import tasks._
import tasks.util._
import tasks.util.eq._

import java.net._
import java.io.File

object Deployment {

  def pack: File = {
    val tmp = TempFile.createTempFile("package")
    selfpackage.write(tmp)
    tmp
  }

  def script(
      memory: Int,
      gridEngine: ElasticSupport[_, _],
      masterAddress: InetSocketAddress,
      download: URL
  ): String = {
    val downloadScript = s"curl -m 60 $download > package && chmod u+x package"

    val edited = "./package -J-Xmx{RAM}M -Dtasks.elastic.engine={GRID} {EXTRA} -Dhosts.master={MASTER} -Dtasks.fileservice.storageURI={STORAGE}"
      .replaceAllLiterally(
          "{RAM}",
          math
            .max(1, (memory.toDouble * config.global.jvmMaxHeapFactor).toInt)
            .toString)
      .replaceAllLiterally("{EXTRA}", config.global.additionalJavaCommandline)
      .replaceAllLiterally(
          "{MASTER}",
          masterAddress.getHostName + ":" + masterAddress.getPort)
      .replaceAllLiterally("{GRID}", gridEngine.toString)
      .replaceAllLiterally("{STORAGE}", config.global.storageURI.toString)

    s"""
$downloadScript && nohup $edited 1> stdout 2>stderr &
"""

  }
}
