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
object App extends Role
object Queue extends Role
object Worker extends Role

trait HostConfiguration {

  def myAddress: InetSocketAddress

  def myCardinality: Int

  def availableMemory: Int

}

trait MasterSlaveConfiguration {

  def master: InetSocketAddress

  def myRoles: Set[Role]

  def isWorker = myRoles.contains(Worker)
  def isQueue = myRoles.contains(Queue)
  def isApp = myRoles.contains(App)
}

trait HostConfigurationFromConfig extends HostConfiguration {

  implicit def config: TasksConfig

  lazy val myPort = chooseNetworkPort

  lazy val hostname = config.hostName

  lazy val myAddress = new java.net.InetSocketAddress(hostname, myPort)

  lazy val myCardinality = config.hostNumCPU

  lazy val availableMemory = config.hostRAM
}

trait MasterSlaveConfigurationFromConfig extends MasterSlaveConfiguration {
  self: HostConfiguration =>

  def config: TasksConfig

  lazy val master: InetSocketAddress = config.masterAddress.getOrElse(myAddress)

  private lazy val startApp = config.startApp

  lazy val myRoles: Set[Role] =
    if (config.masterAddress.isDefined && !startApp) Set(Worker)
    else if (config.masterAddress.isDefined && startApp) Set(Worker, App)
    else if (myAddress == master && startApp) Set(App, Queue, Worker)
    else Set(Queue)

}

class LocalConfiguration(val myCardinality: Int, val availableMemory: Int)
    extends MasterSlaveConfiguration
    with HostConfiguration {

  override lazy val myRoles = Set(App, Queue, Worker)

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
class MasterSlaveFromConfig(implicit val config: TasksConfig)
    extends HostConfigurationFromConfig
    with MasterSlaveConfigurationFromConfig

/**
  * A master slave configuration for full manual setup.
  */
class ManualMasterSlaveConfiguration(
    val myCardinality: Int,
    val availableMemory: Int,
    roles: Set[Role],
    masterAddress: InetSocketAddress,
    val myAddress: InetSocketAddress,
    val codeAddress: InetSocketAddress
) extends MasterSlaveConfiguration {
  override lazy val myRoles = roles
  override lazy val master = masterAddress
}
