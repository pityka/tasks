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

  def availableCPU: Int

  def availableMemory: Int

  def master: InetSocketAddress

  def myRoles: Set[Role]

  def isWorker = myRoles.contains(Worker)
  def isQueue = myRoles.contains(Queue)
  def isApp = myRoles.contains(App)
}

trait HostConfigurationFromConfig extends HostConfiguration {

  implicit def config: TasksConfig

  lazy val myPort = chooseNetworkPort

  private def myHostname = config.hostName

  lazy val myAddress = new java.net.InetSocketAddress(myHostname, myPort)

  lazy val availableCPU = config.hostNumCPU

  lazy val availableMemory = config.hostRAM

  lazy val master: InetSocketAddress = config.masterAddress.getOrElse(myAddress)

  private def startApp = config.startApp

  private val isMaster = myAddress == master

  lazy val myRoles: Set[Role] =
    if (config.masterAddress.isDefined && !startApp) Set(Worker)
    else if (isMaster && startApp) Set(App, Queue, Worker)
    else if (config.masterAddress.isDefined && startApp) Set(Worker, App)
    else Set(Queue)

}

class LocalConfiguration(val availableCPU: Int, val availableMemory: Int)
    extends HostConfiguration {

  override lazy val myRoles = Set(App, Queue, Worker)

  override lazy val master = new InetSocketAddress("localhost", 0)

  val myAddress = master
}

class LocalConfigurationFromConfig(implicit config: TasksConfig)
    extends LocalConfiguration(config.hostNumCPU, config.hostRAM)

class MasterSlaveFromConfig(implicit val config: TasksConfig)
    extends HostConfigurationFromConfig
