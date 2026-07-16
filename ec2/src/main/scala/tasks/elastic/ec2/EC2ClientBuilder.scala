/*
 * The MIT License
 *
 * Copyright (c) 2016 Istvan Bartha
 */
package tasks.elastic.ec2

import cats.effect._
import org.http4s.ember.client.EmberClientBuilder
import smithy4s.aws._
import smithy4s.aws.kernel.{AwsCredentials, AwsRegion}
import smithy4s.time.Timestamp
import com.amazonaws.ec2.EC2

import software.amazon.awssdk.auth.credentials.{
  DefaultCredentialsProvider,
  AwsSessionCredentials
}
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain

/** Builds a smithy4s EC2 client whose credentials come from the AWS Java SDK v2
  * default credential chain (env → IRSA/ECS → EC2 role → SSO → profile →
  * assume-role). Credentials are re-resolved on every call so short-lived
  * (STS/SSO/IRSA) creds stay fresh.
  */
object EC2ClientBuilder {

  def make(configRegion: String): Resource[IO, EC2[IO]] = {
    val javaCredsProvider = DefaultCredentialsProvider.create()

    val region: IO[AwsRegion] = IO.blocking {
      val configured = if (configRegion.isEmpty) None else Some(configRegion)
      val resolved = configured.orElse(
        Option(DefaultAwsRegionProviderChain.builder().build().getRegion())
          .map(_.id())
      )
      resolved
        .map(AwsRegion(_))
        .getOrElse(
          throw new RuntimeException(
            "No AWS region: set tasks.elastic.aws.region, AWS_REGION, " +
              "or configure a profile with a default region"
          )
        )
    }

    val creds: IO[AwsCredentials] = IO.blocking {
      val c = javaCredsProvider.resolveCredentials()
      val session = c match {
        case s: AwsSessionCredentials => Some(s.sessionToken())
        case _                        => None
      }
      AwsCredentials.Default(c.accessKeyId(), c.secretAccessKey(), session)
    }

    for {
      _ <- Resource.eval(
        IO(scribe.info("EC2ClientBuilder: building smithy4s EC2 client"))
      )
      httpClient <- EmberClientBuilder.default[IO].build
      awsEnv = AwsEnvironment.make[IO](
        httpClient,
        region,
        creds,
        IO(Timestamp.nowUTC())
      )
      ec2 <- AwsClient(EC2, awsEnv)
    } yield ec2
  }
}
