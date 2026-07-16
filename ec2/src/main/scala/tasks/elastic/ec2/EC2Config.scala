/*
 * The MIT License
 *
 * Copyright (c) 2016 Istvan Bartha
 */
package tasks.elastic.ec2

import scala.jdk.CollectionConverters._
import org.ekrich.config.{Config, ConfigException}
import tasks.util.config.ConfigValuesForHostConfiguration

/** Configuration for the EC2 elastic backend.
  *
  * Reads from `tasks.elastic.aws.*` — see core/src/main/resources/reference.conf.
  * Instance-type sizing is discovered via the EC2 DescribeInstanceTypes API at
  * startup — this class only holds a list of candidate instance type names.
  */
final class EC2Config(val raw: Config) extends ConfigValuesForHostConfiguration {

  private val root = "tasks.elastic.aws"

  val awsRegion: String = raw.getString(s"$root.region")

  val spotPrice: Double = raw.getDouble(s"$root.spotPrice")

  /** Hard upper bound on spotPrice to catch config mistakes early. */
  val spotPriceCap: Double = raw.getDouble(s"$root.spotPriceCap")

  val amiID: String = raw.getString(s"$root.ami")

  val securityGroups: List[String] =
    raw
      .getStringList(s"$root.securityGroups")
      .asScala
      .toList
      .filter(_.nonEmpty)
      .distinct

  /** Subnet in which to launch spot instances. Required. */
  val subnetId: String = {
    val s = raw.getString(s"$root.subnetId")
    if (s.isEmpty)
      throw new ConfigException.BadValue(
        s"$root.subnetId",
        "must be non-empty; the EC2 backend requires a subnet"
      )
    s
  }

  val keyName: Option[String] = {
    val s = raw.getString(s"$root.keyName")
    if (s.isEmpty) None else Some(s)
  }

  /** IAM instance profile ARN. Optional. If set, must be an ARN. */
  val iamRoleArn: Option[String] = {
    val s = raw.getString(s"$root.iamRoleArn")
    if (s.isEmpty) None
    else if (!s.startsWith("arn:"))
      throw new ConfigException.BadValue(
        s"$root.iamRoleArn",
        s"must be an ARN (starting with 'arn:'), got: $s"
      )
    else Some(s)
  }

  val placementGroup: Option[String] = {
    val s = raw.getString(s"$root.placementGroup")
    if (s.isEmpty) None else Some(s)
  }

  /** Candidate instance types the backend will request. Sizing (vCPU/mem/gpu/scratch)
    * is discovered via DescribeInstanceTypes — never declared here.
    */
  val candidateInstanceTypes: List[String] = {
    val list = raw
      .getStringList(s"$root.instanceTypes")
      .asScala
      .toList
      .filter(_.nonEmpty)
    if (list.isEmpty)
      throw new ConfigException.BadValue(
        s"$root.instanceTypes",
        "must contain at least one candidate instance type name (e.g. m6i.large)"
      )
    list
  }

  /** Tags applied to created instances and spot requests. */
  val instanceTags: List[(String, String)] =
    raw.getObjectList(s"$root.tags").asScala.toList.map { obj =>
      val cfg = obj.toConfig
      cfg.getString("key") -> cfg.getString("value")
    }

  val terminateMaster: Boolean =
    raw.getBoolean(s"$root.terminateMaster")

  /** Mount point for instance-store NVMe volumes. Worker JVMs launch with
    * `-Djava.io.tmpdir=<this>` so scratch space accounting reflects the mount. */
  val instanceStorageMountPoint: String =
    raw.getString(s"$root.instanceStorageMountPoint")

  override def toString: String =
    s"EC2Config(region=${if (awsRegion.isEmpty) "<default-chain>" else awsRegion}, " +
      s"ami=$amiID, subnet=$subnetId, " +
      s"candidates=${candidateInstanceTypes.mkString(",")}, " +
      s"spotPrice=$spotPrice, iamRoleArn=${iamRoleArn.getOrElse("-")}, " +
      s"mount=$instanceStorageMountPoint)"
}
