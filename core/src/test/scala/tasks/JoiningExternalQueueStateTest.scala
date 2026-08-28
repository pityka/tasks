package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import org.ekrich.config.ConfigFactory
import scala.concurrent.duration._

import tasks.jsonitersupport._
import tasks.queue._
import tasks.util.SessionId
import tasks.util.message.LauncherName

object JoiningExternalQueueStateTest extends TestHelpers {

  val testTask = tasks.Task[Input, Int]("joiningExternalQueueState", 1) {
    in => _ =>
      IO(in.i)
  }

  def config(clearOnJoin: Boolean) = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      tasks.cache.enabled = false
      hosts.numCPU = 2
      tasks.disableRemoting = false
      tasks.addShutdownHook = false
      tasks.clearQueueStateWhenMainProcessJoins = $clearOnJoin
      """
    )
  }

  def mainProcess(
      queueStateRef: Ref[IO, QueueImpl.State],
      clearOnJoin: Boolean
  ): Resource[IO, TaskSystemComponents] =
    Resource
      .eval(cats.effect.kernel.Deferred[IO, cats.effect.ExitCode])
      .flatMap { exitCode =>
        tasks.defaultTaskSystem(
          config = Some(config(clearOnJoin)),
          s3Client = Resource.pure(None),
          elasticSupport = Resource.pure(None),
          externalQueueState =
            Resource.pure(Some(tasks.util.Transaction.fromRef(queueStateRef))),
          exitCode = exitCode
        )
      }
      .map(_._1)

}

class JoiningExternalQueueStateTestSuite extends FunSuite with Matchers {

  import JoiningExternalQueueStateTest._

  private val otherSession = "otherMainSession"

  private val leftoverLauncher =
    LauncherName(SessionId.tag(otherSession, "Launcher-1"))

  private def stateWithLeftovers =
    QueueImpl.State.empty.copy(
      mainProcesses = Set(otherSession),
      knownLaunchers = Map(leftoverLauncher -> None),
      nodes = QueueImpl.State.empty.nodes.copy(cumulativeRequested = 102)
    )

  private def runMainProcess(
      initialState: QueueImpl.State,
      clearOnJoin: Boolean
  ): (Int, QueueImpl.State, List[String]) = {
    val captured = scala.collection.mutable.ArrayBuffer.empty[String]
    val handler = scribe.handler.LogHandler(scribe.Level.Warn) { record =>
      val data = record.data.toList.map { case (key, value) =>
        s"$key=${value()}"
      }
      captured.synchronized {
        val _ = captured += (record.logOutput.plainText :: data).mkString(" ")
      }
    }
    scribe.Logger.root
      .clearHandlers()
      .clearModifiers()
      .withHandler(handler)
      .replace()
    try {
      val program = Ref.of[IO, QueueImpl.State](initialState).flatMap {
        queueStateRef =>
          mainProcess(queueStateRef, clearOnJoin).use { implicit ts =>
            for {
              result <- testTask(Input(1))(tasks.ResourceRequest(1, 500))
              state <- queueStateRef.get
            } yield (result, state)
          }
      }
      val (result, state) = program
        .unsafeRunTimed(90.seconds)
        .getOrElse(throw new RuntimeException("timeout"))
      (result, state, captured.synchronized(captured.toList))
    } finally {
      scribe.Logger.root
        .clearHandlers()
        .clearModifiers()
        .withHandler(minimumLevel = Some(scribe.Level.Warn))
        .replace()
    }
  }

  private def warningsOf(event: String, warnings: List[String]) =
    warnings.filter(_.contains(event))

  test(
    "joining an external queue state which a main process and its launcher are still registered in warns and discards nothing"
  ) {
    val (result, state, warnings) =
      runMainProcess(stateWithLeftovers, clearOnJoin = false)

    val occupied = warningsOf("MainProcessJoinedOccupiedQueueState", warnings)

    occupied should have size 1
    occupied.head should include(otherSession)
    occupied.head should include("known-launchers=1")

    result shouldBe 1
    state.mainProcesses should contain(otherSession)
    state.nodes.cumulativeRequested shouldBe 102
  }

  test("joining an empty external queue state does not warn") {
    val (_, _, warnings) =
      runMainProcess(QueueImpl.State.empty, clearOnJoin = false)

    warningsOf("MainProcessJoinedOccupiedQueueState", warnings) shouldBe empty
  }

  test(
    "with clearing on join the arriving main process empties the external queue state"
  ) {
    val (result, state, warnings) =
      runMainProcess(stateWithLeftovers, clearOnJoin = true)

    val cleared = warningsOf("QueueStateClearedOnJoin", warnings)

    cleared should have size 1
    cleared.head should include(otherSession)
    cleared.head should include("discarded-known-launchers=1")
    cleared.head should include("discarded-cumulative-requested=102")

    result shouldBe 1
    state.mainProcesses should have size 1
    state.mainProcesses should not contain otherSession
    state.knownLaunchers.keySet should not contain leftoverLauncher
    state.nodes.cumulativeRequested shouldBe 0

    warningsOf("MainProcessJoinedOccupiedQueueState", warnings) shouldBe empty
  }

  test("clearing an already empty external queue state does not warn") {
    val (_, state, warnings) =
      runMainProcess(QueueImpl.State.empty, clearOnJoin = true)

    state.mainProcesses should have size 1
    warningsOf("QueueStateClearedOnJoin", warnings) shouldBe empty
  }

}
