/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Modified work, Copyright (c) 2016 Istvan Bartha

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

package tasks.deploy

import tasks.util._
import tasks.util.config._
import tasks._


import java.net.InetSocketAddress


sealed trait Role
object MASTER extends Role
object SLAVE extends Role

trait HostConfiguration {

  val myAddress: InetSocketAddress

  val myCardinality: Int

  val availableMemory: Int

}

trait MasterSlaveConfiguration extends HostConfiguration {

  lazy val master: InetSocketAddress =
    if (System.getProperty("hosts.master", "") != "") {
      val h = System.getProperty("hosts.master").split(":")(0)
      val p = System.getProperty("hosts.master").split(":")(1).toInt
      new InetSocketAddress(h, p)
    } else myAddress

  lazy val myRole =
    if (System.getProperty("hosts.master", "") != "") SLAVE
    else {
      if (myAddress == master) MASTER else SLAVE
    }

}

class LocalConfiguration(val myCardinality: Int, val availableMemory: Int)
    extends MasterSlaveConfiguration {

  override lazy val myRole = MASTER

  override lazy val master = new InetSocketAddress("localhost", 0)

  val myAddress = master
}

class LocalConfigurationFromConfig(implicit config: TasksConfig)
    extends LocalConfiguration(config.hostNumCPU, config.hostRAM)

/**
  * Needs a hosts.master system property to infer master location and role
  * Self address is bind to the hosts.hostname config.global.
  * Port is chosen automatically.
  * Cardinality is determined from hosts.numCPU config
  */
class MasterSlaveFromConfig(implicit config: TasksConfig)
    extends MasterSlaveConfiguration {

  val myPort = chooseNetworkPort

  val hostname = config.hostName

  val myAddress = new java.net.InetSocketAddress(hostname, myPort)

  val myCardinality = config.hostNumCPU

  val availableMemory = config.hostRAM

}

/**
  * A master slave configuration for full manual setup.
  */
class ManualMasterSlaveConfiguration(
    val myCardinality: Int,
    val availableMemory: Int,
    role: Role,
    masterAddress: InetSocketAddress,
    val myAddress: InetSocketAddress
) extends MasterSlaveConfiguration {
  override lazy val myRole = role
  override lazy val master = masterAddress
}

trait EC2HostConfiguration extends HostConfiguration {

  implicit def config: TasksConfig

  private val myPort = chooseNetworkPort

  private val myhostname = EC2Operations.readMetadata("local-hostname").head

  lazy val myAddress = new InetSocketAddress(myhostname, myPort)

  private lazy val instancetype = EC2Operations.currentInstanceType

  lazy val availableMemory = instancetype._2.memory

  lazy val myCardinality = instancetype._2.cpu

}

class EC2MasterSlave(implicit val config: TasksConfig)
    extends MasterSlaveConfiguration
    with EC2HostConfiguration
