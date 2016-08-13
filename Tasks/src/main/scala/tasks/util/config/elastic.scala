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

import tasks._

trait TaskAllocationConstants {

  val raw: com.typesafe.config.Config

  val sshHosts = raw.getObject("tasks.elastic.ssh.hosts")

  val elasticNodeAllocationEnabled = raw.getBoolean("tasks.elastic.enabled")

  val tarball =
    if (raw.hasPath("tasks.elastic.tarball"))
      Some(new java.net.URL(raw.getString("tasks.elastic.tarball")))
    else None

  val gridEngine = raw.getString("hosts.gridengine") match {
    case x if x == "EC2" => EC2Grid
    case x if x == "SSH" => SSHGrid
    case _ => NoGrid
  }

  val launchWithDocker = if (raw.hasPath("tasks.elastic.docker")) {
    Some(
        raw.getString("tasks.elastic.docker.registry") -> raw.getString(
            "tasks.elastic.docker.image"))
  } else None

  val idleNodeTimeout: FD = raw.getDuration("tasks.elastic.idleNodeTimeout")

  val maxNodes = raw.getInt("tasks.elastic.maxNodes")

  val maxPendingNodes = raw.getInt("tasks.elastic.maxPending")

  val newNodeJarPath =
    raw.getString("tasks.elastic.mainClassWithClassPathOrJar")

  val queueCheckInterval: FD =
    raw.getDuration("tasks.elastic.queueCheckInterval")

  val queueCheckInitialDelay: FD =
    raw.getDuration("tasks.elastic.queueCheckInitialDelay")

  val nodeKillerMonitorInterval: FD =
    raw.getDuration("tasks.elastic.nodeKillerMonitorInterval")

  val jvmMaxHeapFactor = raw.getDouble("tasks.elastic.jvmMaxHeapFactor")

  val logQueueStatus = raw.getBoolean("tasks.elastic.logQueueStatus")

  val additionalSystemProperties: List[String] = raw
    .getStringList("tasks.elastic.additionalSystemProperties")
    .toArray
    .map(_.asInstanceOf[String])
    .toList

  val emailAddress = raw.getString("tasks.elastic.email")

}
