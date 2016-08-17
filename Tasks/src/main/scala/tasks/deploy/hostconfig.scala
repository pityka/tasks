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

import tasks.caching.kvstore._
import tasks.caching._
import tasks.queue._
import tasks.util._
import tasks.fileservice._
import tasks._

import akka.actor.Actor._
import akka.pattern.ask
import akka.util.Timeout

import java.io.File
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.InetAddress

import scala.concurrent.duration._
import scala.concurrent._
import scala.concurrent.Await
import scala.collection.JavaConversions._

sealed trait Role
object MASTER extends Role
object SLAVE extends Role

trait HostConfiguration {

  val myAddress: InetSocketAddress

  val myCardinality: Int

  val availableMemory: Int

}

trait CacheHostConfiguration {

  val cacheAddress: Option[InetSocketAddress]

}

trait MasterSlaveConfiguration
    extends HostConfiguration
    with CacheHostConfiguration {

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

  lazy val cacheAddress: Option[InetSocketAddress] =
    if (config.global.remoteCacheAddress != "none") {
      val h = config.global.remoteCacheAddress.split(":")(0)
      val p = config.global.remoteCacheAddress.split(":")(1).toInt
      Some(new InetSocketAddress(h, p))
    } else None

}

class LocalConfiguration(val myCardinality: Int, val availableMemory: Int)
    extends MasterSlaveConfiguration {

  override lazy val myRole = MASTER

  override lazy val master = new InetSocketAddress("localhost", 0)

  val myAddress = master
}

object LocalConfigurationFromConfig
    extends LocalConfiguration(config.global.hostNumCPU, config.global.hostRAM)

/**
  * Needs a hosts.master system property to infer master location and role
  * Self address is bind to the hosts.hostname config.global.
  * Port is chosen automatically.
  * Cardinality is determined from hosts.numCPU config
  */
object MasterSlaveFromConfig extends MasterSlaveConfiguration {

  val myPort = chooseNetworkPort

  val hostname = config.global.hostName

  val myAddress = new java.net.InetSocketAddress(hostname, myPort)

  val myCardinality = config.global.hostNumCPU

  val availableMemory = config.global.hostRAM

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

  private val myPort = chooseNetworkPort

  private val myhostname = EC2Operations.readMetadata("local-hostname").head

  lazy val myAddress = new InetSocketAddress(myhostname, myPort)

  private lazy val instancetype = EC2Operations.currentInstanceType

  lazy val availableMemory = instancetype._2.memory

  lazy val myCardinality = instancetype._2.cpu

}

object EC2MasterSlave
    extends MasterSlaveConfiguration
    with EC2HostConfiguration
