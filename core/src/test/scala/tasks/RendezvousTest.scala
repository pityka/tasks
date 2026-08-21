package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should._
import org.scalatest._
import org.ekrich.config.ConfigFactory

import tasks.jsonitersupport._

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global

import tasks.queue.QueueImpl
import tasks.shared.ResourceAvailable
import tasks.util.LocalMessenger
import tasks.util.message.RendezvousGroupId

object RendezvousTest {
  case class RvIn(groupId: String, rank: Int, worldSize: Int, payload: String)
  object RvIn {
    implicit val codec: JsonValueCodec[RvIn] = JsonCodecMaker.make
  }
  case class RvOut(peers: List[String])
  object RvOut {
    implicit val codec: JsonValueCodec[RvOut] = JsonCodecMaker.make
  }

  val rvTask: TaskDefinition[RvIn, RvOut] =
    Task[RvIn, RvOut]("rendezvous", 1) { case RvIn(gid, r, ws, p) =>
      implicit ce => rendezvous(gid, r, ws, p).map(RvOut(_))
    }
}

class RendezvousTestSuite
    extends FunSuite
    with Matchers
    with BeforeAndAfterAll
    with TestHelpers {

  override val testConfig = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""
tasks.cache.enabled = false
tasks.disableRemoting = true
hosts.numCPU = 16
tasks.askInterval = 20 ms
tasks.rendezvousPollInterval = 20 ms
tasks.fileservice.storageURI=${tmp.getAbsolutePath}
"""
    )
  }

  val pair = defaultTaskSystem(Some(testConfig)).allocated.unsafeRunSync()
  implicit val system: TaskSystemComponents = pair._1._1
  import RendezvousTest._

  test("N ranks rendezvous and get peer list ordered by rank") {
    val worldSize = 4
    val groupId = "grp-a"
    val results = IO
      .parSequenceN(worldSize)(
        (0 until worldSize).toList.map { r =>
          rvTask(RvIn(groupId, r, worldSize, s"host-$r:1000"))(
            ResourceRequest(cpu = (1, 1), memory = 1)
          )
        }
      )
      .unsafeRunSync()

    results.foreach { r =>
      r.peers shouldBe (0 until worldSize).toList.map(i => s"host-$i:1000")
    }
  }

  test("two disjoint groups do not interfere") {
    val worldSize = 3
    val call = (gid: String, host: String) =>
      IO.parSequenceN(worldSize)(
        (0 until worldSize).toList.map { r =>
          rvTask(RvIn(gid, r, worldSize, s"$host-$r:1000"))(
            ResourceRequest(cpu = (1, 1), memory = 1)
          )
        }
      )

    val (as, bs) = (call("grp-b", "alpha"), call("grp-c", "beta"))
      .parMapN { case (a, b) =>
        (a, b)
      }
      .unsafeRunSync()

    as.foreach { r =>
      r.peers shouldBe (0 until worldSize).toList.map(i => s"alpha-$i:1000")
    }
    bs.foreach { r =>
      r.peers shouldBe (0 until worldSize).toList.map(i => s"beta-$i:1000")
    }
  }

  override def afterAll() = {
    pair._2.unsafeRunSync()
  }
}

class RendezvousInvariantSuite extends FunSuite with Matchers {

  implicit val config: tasks.util.config.TasksConfig =
    tasks.util.config.parse(() => org.ekrich.config.ConfigFactory.load())

  private def withQueue[A](
      body: (QueueImpl, Ref[IO, Int]) => IO[A]
  ): (A, Int) = {
    val program = Ref.of[IO, Int](0).flatMap { fatalRef =>
      LocalMessenger.make
        .flatMap { messenger =>
          QueueImpl
            .initRef(
              cache = null,
              messenger = messenger,
              shutdownNode = None,
              decideNewNode = None,
              createNode = None,
              convertRunningToPending = None,
              unmanagedResource = ResourceAvailable.empty,
              meterProvider =
                org.typelevel.otel4s.metrics.MeterProvider.noop[IO],
              mainProcessSession = None,
              onFatalError = fatalRef.update(_ + 1)
            )
        }
        .use(q => body(q, fatalRef).flatMap(a => fatalRef.get.map(c => (a, c))))
    }
    program
      .unsafeRunTimed(scala.concurrent.duration.DurationInt(10).seconds)
      .getOrElse(throw new RuntimeException("timeout"))
  }

  test("duplicate rank fails fast and triggers onFatalError") {
    val (secondErr, fatalCount) = withQueue { (q, _) =>
      for {
        firstFiber <- q.rendezvous(RendezvousGroupId("dup"), 0, 2, "a").start
        _ <- IO.sleep(scala.concurrent.duration.DurationInt(100).millis)
        second <- q.rendezvous(RendezvousGroupId("dup"), 0, 2, "b").attempt
        _ <- firstFiber.cancel
      } yield second
    }

    fatalCount shouldBe 1
    secondErr.isLeft shouldBe true
    secondErr.left.map(_.getMessage) shouldBe Left(
      "duplicate rank 0 in group dup"
    )
  }

  test("worldSize mismatch fails fast") {
    val (result, fatalCount) = withQueue { (q, _) =>
      for {
        firstFiber <- q
          .rendezvous(RendezvousGroupId("mismatch"), 0, 2, "a")
          .start
        _ <- IO.sleep(scala.concurrent.duration.DurationInt(100).millis)
        second <- q.rendezvous(RendezvousGroupId("mismatch"), 1, 3, "b").attempt
        _ <- firstFiber.cancel
      } yield second
    }

    fatalCount shouldBe 1
    result.isLeft shouldBe true
    result.left.map(_.getMessage.contains("worldSize mismatch")) shouldBe Left(
      true
    )
  }

  test("rank out of range fails fast") {
    val (result, fatalCount) = withQueue { (q, _) =>
      q.rendezvous(RendezvousGroupId("oor"), rank = 5, worldSize = 2, "a")
        .attempt
    }
    fatalCount shouldBe 1
    result.isLeft shouldBe true
    result.left.map(_.getMessage.contains("out of range")) shouldBe Left(true)
  }

  test("in-process rendezvous returns peers to all joiners") {
    val (result, fatalCount) = withQueue { (q, _) =>
      IO.parSequenceN(3)(
        List(
          q.rendezvous(RendezvousGroupId("g"), 0, 3, "a"),
          q.rendezvous(RendezvousGroupId("g"), 1, 3, "b"),
          q.rendezvous(RendezvousGroupId("g"), 2, 3, "c")
        )
      )
    }
    fatalCount shouldBe 0
    result.foreach(_ shouldBe List("a", "b", "c"))
  }
}
