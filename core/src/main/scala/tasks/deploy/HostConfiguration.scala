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

sealed trait Role
object App extends Role
object Queue extends Role
object Worker extends Role

trait HostConfiguration {

  /** address to which we bind the application sometimes called internal address
    */
  def myAddressBind: SimpleSocketAddress

  /* address seen from outside
   * sometimes called logical or canonical address
   * if empty use bind address
   */
  def myAddressExternal: Option[SimpleSocketAddress]

  def availableCPU: Int

  def availableGPU: List[Int]

  def availableMemory: Int

  def availableScratch: Int

  def image: Option[String]

  def master: SimpleSocketAddress

  def myRoles: Set[Role]

  def isWorker = myRoles.contains(Worker)
  def isQueue = myRoles.contains(Queue)
  def isApp = myRoles.contains(App)
}

trait HostConfigurationFromConfig extends HostConfiguration {

  implicit def config: TasksConfig

  lazy val myPort = chooseNetworkPort

  private def myHostname = config.hostName

  lazy val myAddress = SimpleSocketAddress(myHostname, myPort)

  def myAddressBind: SimpleSocketAddress = myAddress

  def myAddressExternal: Option[SimpleSocketAddress] =
    config.hostNameExternal.map{v =>
      val spl = v.split(':')
      SimpleSocketAddress(spl.head, if (spl.size > 1) spl(1).toInt else myPort)
    }

  lazy val availableCPU = config.hostNumCPU

  lazy val availableGPU = config.hostGPU

  lazy val availableMemory = config.hostRAM

  lazy val image = config.hostImage

  lazy val availableScratch = config.hostScratch

  lazy val master =
    config.masterAddress.getOrElse(myAddressExternal.getOrElse(myAddress))

  private def startApp = config.startApp

  private val isMaster = myAddress == master || myAddressExternal.exists(_ == master)

  lazy val myRoles: Set[Role] =
    if (config.masterAddress.isDefined && !startApp) Set(Worker)
    else if (isMaster && startApp) Set(App, Queue, Worker)
    else if (config.masterAddress.isDefined && startApp) Set(Worker, App)
    else Set(Queue)

}

class LocalConfiguration(
    val availableCPU: Int,
    val availableMemory: Int,
    val availableScratch: Int,
    val availableGPU: List[Int]
) extends HostConfiguration {

  def image = Option.empty[String]

  override val myRoles = Set(App, Queue, Worker)

  override val master = SimpleSocketAddress("localhost", 0)

  def myAddress = master
  def myAddressBind: SimpleSocketAddress = master
  def myAddressExternal: Option[SimpleSocketAddress] = None

}

class LocalConfigurationFromConfig(implicit config: TasksConfig)
    extends LocalConfiguration(
      config.hostNumCPU,
      config.hostRAM,
      config.hostScratch,
      config.hostGPU
    )

class MasterSlaveFromConfig(implicit val config: TasksConfig)
    extends HostConfigurationFromConfig
