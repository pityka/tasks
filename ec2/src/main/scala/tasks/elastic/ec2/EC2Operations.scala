/*
 * The MIT License
 *
 * Copyright (c) 2016 Istvan Bartha
 */
package tasks.elastic.ec2

import cats.effect._
import cats.syntax.all._
import com.amazonaws.ec2._

/** High-level, logged wrappers around the smithy4s EC2 client.
  *
  * Wraps the raw API with:
  *   - pagination of the `Describe*` calls that need it,
  *   - server-side filter usage (never client-side scans),
  *   - scribe logging for every entry / error so failures are traceable.
  */
trait EC2Operations {

  def describeInstanceTypes(names: List[String]): IO[List[InstanceTypeInfo]]

  /** Look up the spot request that spawned a given instance ID, using a
    * server-side filter (never a full-account scan). Paginates.
    */
  def describeSpotRequestByInstance(
      instanceId: String
  ): IO[Option[SpotInstanceRequest]]

  def describeSpotRequestById(
      spotRequestId: String
  ): IO[Option[SpotInstanceRequest]]

  def cancelSpotRequest(spotRequestId: String): IO[Unit]

  def terminateInstance(id: String): IO[Unit]

  def createTags(
      resourceIds: List[String],
      tags: List[(String, String)]
  ): IO[Unit]

  def requestSpotInstance(
      launch: RequestSpotLaunchSpecification,
      spotPrice: Option[String],
      tags: List[(String, String)]
  ): IO[String]
}

object EC2Operations {

  def fromClient(ec2: EC2[IO]): EC2Operations = new FromSmithyClient(ec2)

  private final class FromSmithyClient(ec2: EC2[IO]) extends EC2Operations {

    def describeInstanceTypes(
        names: List[String]
    ): IO[List[InstanceTypeInfo]] = {
      val types = names.map(InstanceType.fromStringOrUnknown)
      def loop(
          acc: List[InstanceTypeInfo],
          next: Option[NextToken]
      ): IO[List[InstanceTypeInfo]] =
        ec2
          .describeInstanceTypes(instanceTypes = Some(types), nextToken = next)
          .flatMap { r =>
            val chunk = r.instanceTypes.getOrElse(Nil)
            r.nextToken match {
              case Some(t) => loop(acc ++ chunk, Some(t))
              case None    => IO.pure(acc ++ chunk)
            }
          }
      IO(scribe.info(s"describeInstanceTypes(${names.mkString(",")})")) *>
        loop(Nil, None)
    }

    def terminateInstance(id: String): IO[Unit] =
      IO(scribe.info(s"terminateInstances(id=$id)")) *>
        ec2
          .terminateInstances(instanceIds = List(InstanceId(id)))
          .void
          .onError { case e =>
            IO(scribe.error(s"terminateInstances($id) failed", e))
          }

    def cancelSpotRequest(spotRequestId: String): IO[Unit] =
      IO(scribe.info(s"cancelSpotInstanceRequests($spotRequestId)")) *>
        ec2
          .cancelSpotInstanceRequests(spotInstanceRequestIds =
            List(SpotInstanceRequestId(spotRequestId))
          )
          .void
          .onError { case e =>
            IO(
              scribe
                .error(s"cancelSpotInstanceRequests($spotRequestId) failed", e)
            )
          }

    def describeSpotRequestByInstance(
        instanceId: String
    ): IO[Option[SpotInstanceRequest]] = {
      val filter = Filter(
        name = Some("instance-id"),
        values = Some(List(instanceId))
      )
      def loop(next: Option[String]): IO[Option[SpotInstanceRequest]] =
        ec2
          .describeSpotInstanceRequests(
            nextToken = next,
            filters = Some(List(filter))
          )
          .flatMap { r =>
            val hit = r.spotInstanceRequests.getOrElse(Nil).headOption
            hit match {
              case Some(_) => IO.pure(hit)
              case None =>
                r.nextToken match {
                  case Some(_) => loop(r.nextToken)
                  case None    => IO.pure(None)
                }
            }
          }
      IO(
        scribe.info(s"describeSpotInstanceRequests(instance-id=$instanceId)")
      ) *> loop(None)
    }

    def describeSpotRequestById(
        spotRequestId: String
    ): IO[Option[SpotInstanceRequest]] =
      IO(
        scribe.info(s"describeSpotInstanceRequests(id=$spotRequestId)")
      ) *>
        ec2
          .describeSpotInstanceRequests(
            spotInstanceRequestIds =
              Some(List(SpotInstanceRequestId(spotRequestId)))
          )
          .map(_.spotInstanceRequests.getOrElse(Nil).headOption)

    def createTags(
        resourceIds: List[String],
        tags: List[(String, String)]
    ): IO[Unit] =
      if (tags.isEmpty || resourceIds.isEmpty) IO.unit
      else {
        val tagList = tags.map { case (k, v) => Tag(Some(k), Some(v)) }
        IO(
          scribe.info(
            s"createTags(resources=${resourceIds.mkString(",")}, tags=${tagList.size})"
          )
        ) *>
          ec2
            .createTags(
              resources = resourceIds.map(TaggableResourceId.apply),
              tags = tagList
            )
            .void
            .onError { case e =>
              IO(scribe.error(s"createTags failed", e))
            }
      }

    def requestSpotInstance(
        launch: RequestSpotLaunchSpecification,
        spotPrice: Option[String],
        tags: List[(String, String)]
    ): IO[String] = {
      val tagSpec =
        if (tags.isEmpty) None
        else
          Some(
            List(
              TagSpecification(
                resourceType = Some(ResourceType.spot_instances_request),
                tags = Some(tags.map { case (k, v) => Tag(Some(k), Some(v)) })
              )
            )
          )
      IO(
        scribe.info(
          s"requestSpotInstances(spotPrice=$spotPrice, tags=${tags.size})"
        )
      ) *>
        ec2
          .requestSpotInstances(
            launchSpecification = Some(launch),
            tagSpecifications = tagSpec,
            instanceCount = Some(1),
            _type = Some(SpotInstanceType.one_time),
            spotPrice = spotPrice
          )
          .flatMap { r =>
            r.spotInstanceRequests
              .getOrElse(Nil)
              .flatMap(_.spotInstanceRequestId)
              .headOption match {
              case Some(id) => IO.pure(id)
              case None =>
                IO.raiseError(
                  new RuntimeException(
                    "RequestSpotInstances returned no spot request id"
                  )
                )
            }
          }
    }
  }
}
