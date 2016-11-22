package tasks.util

import com.bluelabs.akkaaws._
import com.bluelabs.s3stream._

object S3Helpers {

  implicit def credentials: AWSCredentials = {
    val c =
      (new com.amazonaws.auth.DefaultAWSCredentialsProviderChain).getCredentials
    AWSCredentials(c.getAWSAccessKeyId, c.getAWSSecretKey)
  }

}
