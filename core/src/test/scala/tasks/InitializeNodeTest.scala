package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global

import tasks.elastic.{ConvertRunningToPending, CreateNode}
import tasks.queue.QueueImpl
import tasks.shared.{
  CodeVersion,
  PendingJobId,
  ResourceAvailable,
  ResourceRequest,
  VersionedResourceAvailable
}
import tasks.util.LocalMessenger
import tasks.util.config.TasksConfig
import tasks.util.message.{LauncherName, MessageData, Node}

class InitializeNodeTestSuite extends FunSuite with Matchers {

  implicit val config: TasksConfig =
    tasks.util.config.parse(() => org.ekrich.config.ConfigFactory.load())

  private val resource = ResourceAvailable(
    cpu = 1,
    memory = 100,
    scratch = 0,
    gpu = Nil,
    image = None
  )

  private val launcher = LauncherName("test-launcher")

  private val node = Node(tasks.shared.RunningJobId("i-1"), resource, launcher)

  private val availableResource =
    VersionedResourceAvailable(CodeVersion("test"), resource)

  private class RecordingCreateNode(fail: Boolean) extends CreateNode {
    val initialized: Ref[IO, List[Node]] = Ref.unsafe(Nil)

    def requestOneNewJobFromJobScheduler(
        k: ResourceRequest
    )(implicit taskConfig: TasksConfig)
        : IO[Either[String, (PendingJobId, ResourceAvailable)]] =
      IO.raiseError(new RuntimeException("not expected in this test"))

    override def initializeNode(n: Node): IO[Unit] =
      initialized.update(n :: _) *>
        (if (fail) IO.raiseError(new RuntimeException("tagging failed"))
         else IO.unit)
  }

  private def askForWorkWith(createNode: CreateNode) =
    LocalMessenger.make
      .flatMap { messenger =>
        QueueImpl.initRef(
          cache = null,
          messenger = messenger,
          shutdownNode = None,
          decideNewNode = None,
          createNode = Some(createNode),
          convertRunningToPending = Some(ConvertRunningToPending.identity),
          unmanagedResource = ResourceAvailable.empty,
          meterProvider =
            org.typelevel.otel4s.metrics.MeterProvider.noop[cats.effect.IO]
        )
      }
      .use { q =>
        for {
          first <- q.askForWork(launcher, availableResource, Some(node))
          second <- q.askForWork(launcher, availableResource, Some(node))
          launchers <- q.knownLaunchers
        } yield (first, second, launchers)
      }
      .unsafeRunSync()

  test("the elastic backend is asked to initialize a node which came up") {
    val createNode = new RecordingCreateNode(fail = false)

    val (first, _, launchers) = askForWorkWith(createNode)

    first shouldBe Left(MessageData.NothingForSchedule)
    launchers.keySet should contain(launcher)
    createNode.initialized.get.unsafeRunSync() shouldBe List(node)
  }

  test("a node is initialized only on its first contact") {
    val createNode = new RecordingCreateNode(fail = false)

    askForWorkWith(createNode)

    createNode.initialized.get.unsafeRunSync() should have size 1
  }

  test("a failing initializeNode does not fail askForWork") {
    val createNode = new RecordingCreateNode(fail = true)

    val (first, _, launchers) = askForWorkWith(createNode)

    first shouldBe Left(MessageData.NothingForSchedule)
    launchers.keySet should contain(launcher)
    createNode.initialized.get.unsafeRunSync() should have size 1
  }
}
