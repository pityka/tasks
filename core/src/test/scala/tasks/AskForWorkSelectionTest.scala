package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import tasks.fileservice.FileServicePrefix
import tasks.queue._
import tasks.shared._
import tasks.util.LocalMessenger
import tasks.util.message._

class AskForWorkSelectionTest extends FunSuite with Matchers {

  implicit val config: tasks.util.config.TasksConfig =
    tasks.util.config.parse(() => org.ekrich.config.ConfigFactory.load())

  private val cv = CodeVersion("test")

  private val launcher = LauncherName("test-launcher")

  private def offered(cpu: Int, memory: Int, gpu: List[Int]) =
    VersionedResourceAvailable(
      cv,
      ResourceAvailable(
        cpu = cpu,
        memory = memory,
        scratch = 0,
        gpu = gpu,
        image = None
      )
    )

  private def task(
      name: String,
      priority: Int,
      cpu: Int,
      memory: Int,
      gpu: Int
  ) =
    MessageData.ScheduleTask(
      description = HashedTaskDescription(TaskId(name, 1), name + "-hash"),
      inputDeserializer = Spore[AnyRef, AnyRef]("some.pkg.Deserializer$", Nil),
      outputSerializer = Spore[AnyRef, AnyRef]("some.pkg.Serializer$", Nil),
      function = Spore[AnyRef, AnyRef]("some.pkg.Jobs$body$1", Nil),
      resource = VersionedResourceRequest(
        cv,
        ResourceRequest((cpu, cpu), memory, 0, gpu, None)
      ),
      input = MessageData.InputData(Base64Data("aGVsbG8="), false),
      fileServicePrefix = FileServicePrefix(Vector("prefix")),
      tryCache = false,
      priority = Priority(priority),
      labels = Labels(Nil),
      lineage = TaskLineage(Nil),
      proxy = Address("proxy-" + name, None),
      filePrefix = "file-prefix"
    )

  private def scheduledTaskIdOf(
      queued: List[MessageData.ScheduleTask],
      available: VersionedResourceAvailable
  ): Option[String] = {
    val program = LocalMessenger.make
      .flatMap { messenger =>
        QueueImpl.initRef(
          cache = null,
          messenger = messenger,
          shutdownNode = None,
          decideNewNode = None,
          createNode = None,
          convertRunningToPending = None,
          unmanagedResource = ResourceAvailable.empty,
          meterProvider = org.typelevel.otel4s.metrics.MeterProvider.noop[IO],
          mainProcessSession = None
        )
      }
      .use { q =>
        queued
          .foldLeft(IO.unit)((acc, sch) => acc *> q.scheduleTask(sch))
          .flatMap(_ => q.askForWork(launcher, available, None))
      }

    program.unsafeRunSync() match {
      case Right(MessageData.Schedule(sch)) =>
        Some(sch.description.taskId.id)
      case Left(_) => None
    }
  }

  test("among tasks of equal priority the largest one that fits is taken") {
    val small = task("small", priority = 0, cpu = 1, memory = 500, gpu = 0)
    val large = task("large", priority = 0, cpu = 4, memory = 2000, gpu = 0)

    scheduledTaskIdOf(
      List(small, large),
      offered(cpu = 8, memory = 4000, gpu = Nil)
    ) shouldBe Some("large")

    scheduledTaskIdOf(
      List(large, small),
      offered(cpu = 8, memory = 4000, gpu = Nil)
    ) shouldBe Some("large")
  }

  test("a gpu task outranks a larger cpu-only task of the same priority") {
    val cpuOnly = task("cpuOnly", priority = 0, cpu = 8, memory = 4000, gpu = 0)
    val gpuTask = task("gpuTask", priority = 0, cpu = 1, memory = 500, gpu = 2)

    scheduledTaskIdOf(
      List(cpuOnly, gpuTask),
      offered(cpu = 8, memory = 4000, gpu = List(0, 1))
    ) shouldBe Some("gpuTask")
  }

  test("priority still wins over size") {
    val small = task("small", priority = 10, cpu = 1, memory = 500, gpu = 0)
    val large = task("large", priority = 0, cpu = 4, memory = 2000, gpu = 0)

    scheduledTaskIdOf(
      List(small, large),
      offered(cpu = 8, memory = 4000, gpu = Nil)
    ) shouldBe Some("small")
  }

  test("a task that does not fit is skipped in favour of one that does") {
    val tooBig = task("tooBig", priority = 0, cpu = 32, memory = 4000, gpu = 0)
    val fits = task("fits", priority = 0, cpu = 2, memory = 1000, gpu = 0)

    scheduledTaskIdOf(
      List(tooBig, fits),
      offered(cpu = 8, memory = 4000, gpu = Nil)
    ) shouldBe Some("fits")
  }

}
