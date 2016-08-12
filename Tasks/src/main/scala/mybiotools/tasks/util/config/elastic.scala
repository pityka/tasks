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

trait TaskAllocationConstants {

  val tarball =
    if (global.hasPath("tasks.elastic.tarball"))
      Some(new java.net.URL(global.getString("tasks.elastic.tarball")))
    else None

  val gridEngine = global.getString("hosts.gridengine")

  val launchWithDocker = if (global.hasPath("tasks.elastic.docker")) {
    Some(
        global.getString("tasks.elastic.docker.registry") -> global.getString(
            "tasks.elastic.docker.image"))
  } else None

  val idleNodeTimeout: FD = global.getDuration("tasks.elastic.idleNodeTimeout")

  val maxNodes = global.getInt("tasks.elastic.maxNodes")

  val maxPendingNodes = global.getInt("tasks.elastic.maxPending")

  val newNodeJarPath =
    global.getString("tasks.elastic.mainClassWithClassPathOrJar")

  val queueCheckInterval: FD =
    global.getDuration("tasks.elastic.queueCheckInterval")

  val queueCheckInitialDelay: FD =
    global.getDuration("tasks.elastic.queueCheckInitialDelay")

  val nodeKillerMonitorInterval: FD =
    global.getDuration("tasks.elastic.nodeKillerMonitorInterval")

  val jVMMaxHeapFactor = global.getDouble("tasks.elastic.jvmMaxHeapFactor")

  val logQueueStatus = global.getBoolean("tasks.elastic.logQueueStatus")

  val additionalSystemProperties: List[String] = global
    .getStringList("tasks.elastic.additionalSystemProperties")
    .toArray
    .map(_.asInstanceOf[String])
    .toList

  val emailAddress = global.getString("tasks.elastic.email")

}
