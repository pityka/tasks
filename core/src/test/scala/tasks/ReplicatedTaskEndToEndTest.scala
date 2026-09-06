package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should._
import org.scalatest._
import org.ekrich.config.ConfigFactory

import tasks.jsonitersupport._

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import cats.effect.unsafe.implicits.global

import java.util.concurrent.atomic.AtomicInteger

import tasks.shared.Replication

object ReplicatedTaskEndToEndTest {

  val started = new AtomicInteger(0)

  case class In(holdMillis: Int)
  object In {
    implicit val codec: JsonValueCodec[In] = JsonCodecMaker.make
  }

  val replicated: TaskDefinition[In, Int] =
    Task[In, Int]("replicated", 1) { case In(holdMillis) =>
      _ =>
        IO(started.incrementAndGet()).flatMap { nth =>
          if (nth == 1)
            IO.sleep(
              scala.concurrent.duration.FiniteDuration(
                holdMillis.toLong,
                "ms"
              )
            ).as(nth)
          else IO.pure(nth)
        }
    }
}

class ReplicatedTaskEndToEndTestSuite
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
tasks.fileservice.storageURI=${tmp.getAbsolutePath}
"""
    )
  }

  val pair = defaultTaskSystem(Some(testConfig)).allocated.unsafeRunSync()
  implicit val system: TaskSystemComponents = pair._1._1
  import ReplicatedTaskEndToEndTest._

  test("the caller gets the first copy to exit, and more than one copy runs") {
    val result = replicated(In(3000))(
      ResourceRequest(cpu = 1, memory = 1, replication = Replication(3))
    ).unsafeRunTimed(scala.concurrent.duration.DurationInt(60).seconds)
      .getOrElse(fail("timed out"))

    withClue(s"started=${started.get} result=$result ") {
      result should be > 1
      started.get should be > 1
    }
  }

  override def afterAll() = {
    pair._2.unsafeRunSync()
  }
}
