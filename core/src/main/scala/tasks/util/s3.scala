package tasks.util

import com.bluelabs.akkaaws._
import com.bluelabs.s3stream._

object S3Helpers {

  implicit def credentials: AWSCredentials = {
    val c =
      (new com.amazonaws.auth.DefaultAWSCredentialsProviderChain).getCredentials
    c match {

      case c: com.amazonaws.auth.AWSSessionCredentials =>
        AWSSessionCredentials(c.getAWSAccessKeyId,
                              c.getAWSSecretKey,
                              c.getSessionToken)
      case c => AWSCredentials(c.getAWSAccessKeyId, c.getAWSSecretKey)
    }

  }

}
