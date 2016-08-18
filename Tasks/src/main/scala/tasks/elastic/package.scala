package tasks

package object elastic {
  lazy val elasticSupport = tasks.util.config.global.gridEngine match {
    case "EC2" => Some(tasks.elastic.ec2.EC2Grid)
    case "SSH" => Some(tasks.elastic.ssh.SSHGrid)
    case "SH" => Some(tasks.elastic.sh.SHGrid)
    case _ => None
  }
}
