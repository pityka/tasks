package tasks.elastic.ec2

import tasks.deploy._
import tasks.util.config.TasksConfig
import tasks.util.EC2Operations
import tasks.util.SimpleSocketAddress

trait EC2HostConfiguration extends HostConfigurationFromConfig {

  implicit def config: TasksConfig

  private lazy val myhostname =
    EC2Operations.readMetadata("local-hostname").head

  override lazy val myAddress = SimpleSocketAddress(myhostname, myPort)

  private lazy val instancetype = EC2Operations.currentInstanceType

  override lazy val availableMemory = instancetype._2.memory

  override lazy val availableCPU = instancetype._2.cpu

}

class EC2MasterSlave(implicit val config: TasksConfig)
    extends EC2HostConfiguration
