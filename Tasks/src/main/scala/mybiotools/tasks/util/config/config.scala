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

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import scala.concurrent.duration._

package object config
    extends TaskAllocationConstants
    with EC2Settings
    with LSFConfig {

  type FD = FiniteDuration

  implicit def asFiniteDuration(d: java.time.Duration) =
    scala.concurrent.duration.Duration.fromNanos(d.toNanos)

  private[config] val global = ConfigFactory.load()

  def load = ConfigFactory.load()

  val asString = global.root.render

  val proxyTaskGetBackResult: FD =
    global.getDuration("tasks.proxytaskGetBackResultTimeout")

  val launcherActorHeartBeatInterval: FD =
    global.getDuration("tasks.failuredetector.heartbeat-interval")

  val fileSendChunkSize = global.getBytes("tasks.fileSendChunkSize").toInt

  val includeFullPathInDefaultSharedName =
    global.getBoolean("tasks.includeFullPathInDefaultSharedName")

  val resubmitFailedTask = global.getBoolean("tasks.resubmitFailedTask")

  val logToStandardOutput = global.getBoolean("tasks.stdout")

  val verifySharedFileInCache =
    global.getBoolean("tasks.verifySharedFileInCache")

  val disableRemoting = global.getBoolean("tasks.disableRemoting")

  val nonLocalFileSystems = global
    .getStringList("tasks.nonLocalFileSystems")
    .map(f => new java.io.File(f))

  val skipContentHashVerificationAfterCache =
    global.getBoolean("tasks.skipContentHashVerificationAfterCache")

  val acceptableHeartbeatPause: FD =
    global.getDuration("tasks.failuredetector.acceptable-heartbeat-pause")

  val remoteCacheAddress = global.getString("hosts.remoteCacheAddress")

  val hostNumCPU = global.getInt("hosts.numCPU")

  val hostRAM = global.getInt("hosts.RAM")

  val hostName = global.getString("hosts.hostname")

  val hostReservedCPU = global.getInt("hosts.reservedCPU")

  val hostPort = global.getInt("hosts.port")

  val sshHosts = global.getObject("tasks.elastic.ssh.hosts")

}
