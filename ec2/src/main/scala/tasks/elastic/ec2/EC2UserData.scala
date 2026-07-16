/*
 * The MIT License
 *
 * Copyright (c) 2016 Istvan Bartha
 */
package tasks.elastic.ec2

import tasks.util.SimpleSocketAddress
import tasks.util.Uri
import tasks.util.config.TasksConfig
import tasks.elastic.Deployment

/** Builds the user-data script executed by the worker at first boot.
  *
  * Layout:
  *   1. Mount instance-store NVMe volumes (RAID0 if multiple) to `mountPoint`.
  *   2. Point `java.io.tmpdir` at that mount so scratch space reflects the
  *      instance-store size.
  *   3. Download the worker package.
  *   4. Launch the JVM in a subshell.
  *   5. When the JVM exits, terminate this instance via `aws ec2
  *      terminate-instances` using the instance profile — no framework-driven
  *      self-terminate needed.
  *
  * The instance-store mount block is copied from the Trail infra Batch launch
  * template (~/Trail/infra/dev/2-batch/launch_template.tf).
  */
object EC2UserData {

  def script(
      memory: Int,
      cpu: Int,
      scratch: Int,
      gpus: List[Int],
      masterAddress: SimpleSocketAddress,
      masterPrefix: String,
      codeDownload: Uri,
      image: Option[String],
      labels: Set[String],
      mountPoint: String
  )(implicit config: TasksConfig): String = {
    val jvmLaunch = Deployment.script(
      memory = memory,
      cpu = cpu,
      scratch = scratch,
      gpus = gpus,
      masterAddress = masterAddress,
      masterPrefix = masterPrefix,
      download = codeDownload,
      followerHostname = None,
      followerExternalHostname = None,
      followerMayUseArbitraryPort = true,
      followerNodeName = None,
      background = false,
      image = image,
      labels = labels
    )
    val mp = shellQuote(mountPoint)

    s"""#!/usr/bin/env bash
       |set -u
       |exec > /var/log/tasks-worker.log 2>&1
       |
       |MOUNT=$mp
       |# Discover instance-store NVMe devices
       |DEVICES=()
       |for link in /dev/disk/by-id/nvme-Amazon_EC2_NVMe_Instance_Storage_*; do
       |  [ -L "$$link" ] || continue
       |  real=$$(readlink -f "$$link")
       |  [[ "$$real" == *n1 ]] || continue
       |  DEVICES+=("$$real")
       |done
       |readarray -t DEVICES < <(printf '%s\\n' "$${DEVICES[@]}" | sort -u)
       |
       |if [ $${#DEVICES[@]} -gt 0 ]; then
       |  mkdir -p "$$MOUNT"
       |  if [ $${#DEVICES[@]} -eq 1 ]; then
       |    mkfs.xfs -f "$${DEVICES[0]}"
       |    mount "$${DEVICES[0]}" "$$MOUNT"
       |  else
       |    (dnf install -y mdadm || yum install -y mdadm) || true
       |    mdadm --create /dev/md0 --level=0 --raid-devices=$${#DEVICES[@]} "$${DEVICES[@]}"
       |    mkfs.xfs -f /dev/md0
       |    mount /dev/md0 "$$MOUNT"
       |  fi
       |  chmod 1777 "$$MOUNT"
       |fi
       |
       |# Point JVM temp files at the mount so `hosts.scratch` accounting picks it up.
       |export _JAVA_OPTIONS="-Djava.io.tmpdir=$$MOUNT"
       |
       |# Run the worker; when it exits, terminate this instance via IMDSv2 + AWS CLI.
       |(
       |  $jvmLaunch
       |  TOKEN=$$(curl -s -X PUT -H "X-aws-ec2-metadata-token-ttl-seconds: 60" http://169.254.169.254/latest/api/token)
       |  ID=$$(curl -s -H "X-aws-ec2-metadata-token: $$TOKEN" http://169.254.169.254/latest/meta-data/instance-id)
       |  REGION=$$(curl -s -H "X-aws-ec2-metadata-token: $$TOKEN" http://169.254.169.254/latest/meta-data/placement/region)
       |  aws --region "$$REGION" ec2 terminate-instances --instance-ids "$$ID" || shutdown -h now
       |) &
       |""".stripMargin
  }

  /** POSIX single-quote a string for safe interpolation into a bash script. */
  private[ec2] def shellQuote(s: String): String =
    "'" + s.replace("'", "'\"'\"'") + "'"
}
