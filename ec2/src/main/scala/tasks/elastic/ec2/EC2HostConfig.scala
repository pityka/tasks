package tasks.elastic.ec2

import tasks.deploy._
import tasks.util.config.TasksConfig
import tasks.util.chooseNetworkPort
import tasks.util.EC2Operations

import java.net.InetSocketAddress

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
    extends MasterSlaveConfigurationFromConfig
    with EC2HostConfiguration
