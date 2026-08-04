package tasks.elastic.ec2

import cats.effect.IO
import cats.effect.kernel.Ref
import com.amazonaws.ec2._

class FakeEC2Operations(
    spotRequestsByInstance: Map[String, SpotInstanceRequest] = Map.empty,
    spotRequestsById: Map[String, SpotInstanceRequest] = Map.empty,
    lookupFailure: Option[Throwable] = None
) extends EC2Operations {

  val terminated: Ref[IO, List[String]] = Ref.unsafe(Nil)
  val cancelled: Ref[IO, List[String]] = Ref.unsafe(Nil)
  val tagged: Ref[IO, List[(List[String], List[(String, String)])]] =
    Ref.unsafe(Nil)

  def describeInstanceTypes(names: List[String]): IO[List[InstanceTypeInfo]] =
    IO.pure(Nil)

  def describeSpotRequestByInstance(
      instanceId: String
  ): IO[Option[SpotInstanceRequest]] = lookupFailure match {
    case Some(e) => IO.raiseError(e)
    case None    => IO.pure(spotRequestsByInstance.get(instanceId))
  }

  def describeSpotRequestById(
      spotRequestId: String
  ): IO[Option[SpotInstanceRequest]] =
    IO.pure(spotRequestsById.get(spotRequestId))

  def cancelSpotRequest(spotRequestId: String): IO[Unit] =
    cancelled.update(spotRequestId :: _)

  def terminateInstance(id: String): IO[Unit] =
    terminated.update(id :: _)

  def createTags(
      resourceIds: List[String],
      tags: List[(String, String)]
  ): IO[Unit] =
    tagged.update((resourceIds, tags) :: _)

  def requestSpotInstance(
      launch: RequestSpotLaunchSpecification,
      spotPrice: Option[String],
      tags: List[(String, String)]
  ): IO[String] =
    IO.raiseError(new RuntimeException("requestSpotInstance not stubbed"))
}

object FakeEC2Operations {

  def spotRequest(
      spotRequestId: String,
      instanceId: Option[String] = None
  ): SpotInstanceRequest =
    SpotInstanceRequest(
      spotInstanceRequestId = Some(spotRequestId),
      instanceId = instanceId.map(InstanceId.apply)
    )
}
