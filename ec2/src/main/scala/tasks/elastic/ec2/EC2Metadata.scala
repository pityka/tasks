/*
 * The MIT License
 *
 * Copyright (c) 2016 Istvan Bartha
 */
package tasks.elastic.ec2

import cats.effect.{IO, Ref}

/** Minimal IMDSv2 client.
  *
  * Fetches an IMDSv2 session token via `PUT /latest/api/token` and uses it for
  * `GET /latest/meta-data/<key>` calls. Values are cached for the lifetime of
  * the process — the master's instance ID, type and hostname are stable.
  *
  * Uses `java.net.http.HttpClient` (JDK 11+) rather than http4s so that
  * metadata reads can run before the smithy4s stack is wired up.
  */
object EC2Metadata {

  private val base = "http://169.254.169.254"
  private val timeout = java.time.Duration.ofSeconds(2)

  private val httpClient: java.net.http.HttpClient =
    java.net.http.HttpClient.newBuilder().connectTimeout(timeout).build()

  private val cache: Ref[IO, Map[String, String]] =
    Ref.unsafe[IO, Map[String, String]](Map.empty)

  private val tokenCache: Ref[IO, Option[String]] =
    Ref.unsafe[IO, Option[String]](None)

  private def fetchToken: IO[String] = tokenCache.get.flatMap {
    case Some(t) => IO.pure(t)
    case None =>
      IO.blocking {
        val req = java.net.http.HttpRequest
          .newBuilder(java.net.URI.create(s"$base/latest/api/token"))
          .timeout(timeout)
          .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
          .PUT(java.net.http.HttpRequest.BodyPublishers.noBody())
          .build()
        val resp = httpClient.send(
          req,
          java.net.http.HttpResponse.BodyHandlers.ofString()
        )
        if (resp.statusCode() != 200) {
          scribe.error(
            s"IMDSv2 token request failed: HTTP ${resp.statusCode()}"
          )
          throw new RuntimeException(
            s"IMDSv2 token request failed: HTTP ${resp.statusCode()}"
          )
        }
        resp.body()
      }.flatTap(t => tokenCache.set(Some(t)))
  }

  /** Read a metadata key, cached. Throws on failure. */
  def read(key: String): IO[String] = cache.get.flatMap { cached =>
    cached.get(key) match {
      case Some(v) => IO.pure(v)
      case None =>
        for {
          token <- fetchToken
          value <- IO.blocking {
            val req = java.net.http.HttpRequest
              .newBuilder(java.net.URI.create(s"$base/latest/meta-data/$key"))
              .timeout(timeout)
              .header("X-aws-ec2-metadata-token", token)
              .GET()
              .build()
            val resp = httpClient.send(
              req,
              java.net.http.HttpResponse.BodyHandlers.ofString()
            )
            if (resp.statusCode() != 200) {
              scribe.error(
                s"IMDS read of $key failed: HTTP ${resp.statusCode()}"
              )
              throw new RuntimeException(
                s"IMDS read of $key failed: HTTP ${resp.statusCode()}"
              )
            }
            resp.body().trim
          }
          _ <- cache.update(_ + (key -> value))
        } yield value
    }
  }

  def instanceId: IO[String] = read("instance-id")
  def instanceType: IO[String] = read("instance-type")
  def localHostname: IO[String] = read("local-hostname")
  def region: IO[String] = read("placement/region")
}
