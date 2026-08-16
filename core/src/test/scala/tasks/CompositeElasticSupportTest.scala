package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers
import org.ekrich.config.ConfigFactory

import cats.effect.IO
import cats.effect.ExitCode
import cats.effect.kernel.Deferred
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global

import tasks.deploy._
import tasks.elastic._
import tasks.shared._
import tasks.shared.ResourceRequest
import tasks.util.SimpleSocketAddress
import tasks.util.config.TasksConfig

object CompositeElasticSupportTest {

  val masterAddress = SimpleSocketAddress("master", 1234)

  val codeAddress =
    CodeAddress(SimpleSocketAddress("master", 1235), CodeVersion("1"))

  def tasksConfig(extra: String): TasksConfig =
    tasks.util.config.parse(() =>
      tasks.util.loadConfig(Some(ConfigFactory.parseString(extra)))
    )

  def request(memory: Int): ResourceRequest =
    ResourceRequest(
      cpu = (1, 1),
      memory = memory,
      scratch = 0,
      gpu = 0,
      image = None,
      nodeSelector = None
    )

  def available(memory: Int): ResourceAvailable =
    ResourceAvailable(
      cpu = 1,
      memory = memory,
      scratch = 0,
      gpu = Nil,
      image = None
    )

  type Spawn =
    ResourceRequest => IO[Either[String, (PendingJobId, ResourceAvailable)]]

  def spawns(nodeId: String): Spawn =
    request => IO.pure(Right((PendingJobId(nodeId), available(request.memory))))

  def declines(reason: String): Spawn = _ => IO.pure(Left(reason))

  def raises(reason: String): Spawn =
    _ => IO.raiseError(new RuntimeException(reason))

  def raisesEagerly(reason: String): Spawn =
    _ => throw new RuntimeException(reason)

  def fake(
      name: String,
      calls: Ref[IO, List[String]],
      spawn: Spawn,
      workerNodeName: String,
      hostConfig: Option[TasksConfig => HostConfiguration]
  ): ElasticSupport = {

    def record(what: String) = calls.update(_ :+ s"$name:$what")

    new ElasticSupport(
      hostConfig = hostConfig,
      shutdownFromNodeRegistry = new ShutdownNode {
        def shutdownRunningNode(nodeName: RunningJobId): IO[Unit] =
          record(s"shutdownRunning:${nodeName.value}")
        def shutdownPendingNode(nodeName: PendingJobId): IO[Unit] =
          record(s"shutdownPending:${nodeName.value}")
      },
      shutdownFromWorker = new ShutdownSelfNode {
        def shutdownRunningNode(
            exitCode: Deferred[IO, ExitCode],
            nodeName: RunningJobId
        ): IO[Unit] = record(s"shutdownSelf:${nodeName.value}")
      },
      createNodeFactory = new CreateNodeFactory {
        def apply(
            masterAddress: SimpleSocketAddress,
            masterPrefix: String,
            codeAddress: CodeAddress
        ): CreateNode = new CreateNode {
          def requestOneNewJobFromJobScheduler(
              k: ResourceRequest
          )(implicit taskConfig: TasksConfig) =
            record("spawn") *> spawn(k)
          override def convertRunningToPending(
              p: RunningJobId
          ): IO[Option[PendingJobId]] =
            record(s"convert:${p.value}").as(Some(PendingJobId(p.value + "-p")))
        }
      },
      getNodeName = new GetNodeName {
        def getNodeName(config: TasksConfig): IO[RunningJobId] =
          record("getNodeName").as(RunningJobId(workerNodeName))
      },
      convertRunningToPending = new ConvertRunningToPending {
        override def convertRunningToPending(
            p: RunningJobId
        ): IO[Option[PendingJobId]] =
          record(s"convert:${p.value}").as(Some(PendingJobId(p.value + "-p")))
      }
    )
  }

  def fake(name: String, calls: Ref[IO, List[String]]): ElasticSupport =
    fake(name, calls, spawns(name + "-node"), name + "-node", None)

  def createNodeOf(support: ElasticSupport): CreateNode =
    support.createNodeFactory.apply(masterAddress, "prefix", codeAddress)
}

class CompositeElasticSupportTestSuite extends FunSuite with Matchers {
  import CompositeElasticSupportTest._

  implicit val emptyTasksConfig: TasksConfig = tasksConfig("")

  def recorder = Ref.unsafe[IO, List[String]](Nil)

  test("a node request goes to the first member which accepts it") {
    val calls = recorder
    val composite = CompositeElasticSupport(
      List(
        CompositeMember("big", fake("big", calls), _.startsWith("big"))
          .accepting(_.memory >= 1000),
        CompositeMember.catchAll("small", fake("small", calls))
      )
    )

    val result = createNodeOf(composite)
      .requestOneNewJobFromJobScheduler(request(2000))
      .unsafeRunSync()

    result.map(_._1) shouldBe Right(PendingJobId("big-node"))
    calls.get.unsafeRunSync() shouldBe List("big:spawn")
  }

  test("a request no member accepts fails without spawning anything") {
    val calls = recorder
    val composite = CompositeElasticSupport(
      List(
        CompositeMember("big", fake("big", calls), _.startsWith("big"))
          .accepting(_.memory >= 1000)
      )
    )

    val result = createNodeOf(composite)
      .requestOneNewJobFromJobScheduler(request(100))
      .unsafeRunSync()

    result.isLeft shouldBe true
    result.left.getOrElse(fail("expected Left")) should include("big")
    calls.get.unsafeRunSync() shouldBe Nil
  }

  test("a declining member falls through to the next one") {
    val calls = recorder
    val composite = CompositeElasticSupport(
      List(
        CompositeMember(
          "first",
          fake("first", calls, declines("no capacity"), "first-node", None),
          _.startsWith("first")
        ),
        CompositeMember.catchAll("second", fake("second", calls))
      )
    )

    val result = createNodeOf(composite)
      .requestOneNewJobFromJobScheduler(request(100))
      .unsafeRunSync()

    result.map(_._1) shouldBe Right(PendingJobId("second-node"))
    calls.get.unsafeRunSync() shouldBe List("first:spawn", "second:spawn")
  }

  test("a member raising inside its IO falls through to the next one") {
    val calls = recorder
    val composite = CompositeElasticSupport(
      List(
        CompositeMember(
          "first",
          fake("first", calls, raises("boom"), "first-node", None),
          _.startsWith("first")
        ),
        CompositeMember.catchAll("second", fake("second", calls))
      )
    )

    createNodeOf(composite)
      .requestOneNewJobFromJobScheduler(request(100))
      .unsafeRunSync()
      .map(_._1) shouldBe Right(PendingJobId("second-node"))
  }

  test("a member raising before returning an IO falls through to the next one") {
    val calls = recorder
    val composite = CompositeElasticSupport(
      List(
        CompositeMember(
          "first",
          fake("first", calls, raisesEagerly("boom"), "first-node", None),
          _.startsWith("first")
        ),
        CompositeMember.catchAll("second", fake("second", calls))
      )
    )

    createNodeOf(composite)
      .requestOneNewJobFromJobScheduler(request(100))
      .unsafeRunSync()
      .map(_._1) shouldBe Right(PendingJobId("second-node"))
  }

  test("when every member fails the error names each of them") {
    val calls = recorder
    val composite = CompositeElasticSupport(
      List(
        CompositeMember(
          "first",
          fake("first", calls, declines("no capacity"), "first-node", None),
          _.startsWith("first")
        ),
        CompositeMember.catchAll(
          "second",
          fake("second", calls, raises("boom"), "second-node", None)
        )
      )
    )

    val error = createNodeOf(composite)
      .requestOneNewJobFromJobScheduler(request(100))
      .unsafeRunSync()
      .left
      .getOrElse(fail("expected Left"))

    error should include("first: no capacity")
    error should include("second: boom")
  }

  test("shutdown of a running and of a pending node reaches only its owner") {
    val calls = recorder
    val composite = CompositeElasticSupport(
      List(
        CompositeMember("a", fake("a", calls), _.startsWith("a-")),
        CompositeMember.catchAll("b", fake("b", calls))
      )
    )

    composite.shutdownFromNodeRegistry
      .shutdownRunningNode(RunningJobId("a-1"))
      .unsafeRunSync()
    composite.shutdownFromNodeRegistry
      .shutdownPendingNode(PendingJobId("b-1"))
      .unsafeRunSync()

    calls.get.unsafeRunSync() shouldBe List(
      "a:shutdownRunning:a-1",
      "b:shutdownPending:b-1"
    )
  }

  test("two members of the same kind are told apart by their node ids") {
    val calls = recorder
    val clusterOne =
      "arn:aws:ecs:us-east-1:1:task/cluster-one/0123456789abcdef"
    val clusterTwo =
      "arn:aws:ecs:us-east-1:1:task/cluster-two/fedcba9876543210"
    def ownsCluster(cluster: String)(nodeId: String) =
      nodeId.split('/').toList.lift(1).contains(cluster)

    val composite = CompositeElasticSupport(
      List(
        CompositeMember("one", fake("one", calls), ownsCluster("cluster-one")),
        CompositeMember("two", fake("two", calls), ownsCluster("cluster-two"))
      )
    )

    composite.shutdownFromNodeRegistry
      .shutdownRunningNode(RunningJobId(clusterTwo))
      .unsafeRunSync()
    composite.shutdownFromNodeRegistry
      .shutdownRunningNode(RunningJobId(clusterOne))
      .unsafeRunSync()

    calls.get.unsafeRunSync() shouldBe List(
      s"two:shutdownRunning:$clusterTwo",
      s"one:shutdownRunning:$clusterOne"
    )
  }

  test("an unowned node id is never handed to any member") {
    val calls = recorder
    val composite = CompositeElasticSupport(
      List(
        CompositeMember("a", fake("a", calls), _.startsWith("a-")),
        CompositeMember("b", fake("b", calls), _.startsWith("b-"))
      )
    )

    composite.shutdownFromNodeRegistry
      .shutdownRunningNode(RunningJobId("c-1"))
      .unsafeRunSync()
    composite.shutdownFromNodeRegistry
      .shutdownPendingNode(PendingJobId("c-1"))
      .unsafeRunSync()
    composite.convertRunningToPending
      .convertRunningToPending(RunningJobId("c-1"))
      .unsafeRunSync() shouldBe None

    calls.get.unsafeRunSync() shouldBe Nil
  }

  test("a shutdown which throws is contained") {
    val calls = recorder
    val throwing = new ElasticSupport(
      hostConfig = None,
      shutdownFromNodeRegistry = new ShutdownNode {
        def shutdownRunningNode(nodeName: RunningJobId): IO[Unit] =
          throw new RuntimeException("boom")
        def shutdownPendingNode(nodeName: PendingJobId): IO[Unit] =
          throw new RuntimeException("boom")
      },
      shutdownFromWorker = new ShutdownSelfNode {
        def shutdownRunningNode(
            exitCode: Deferred[IO, ExitCode],
            nodeName: RunningJobId
        ): IO[Unit] = IO.unit
      },
      createNodeFactory = new CreateNodeFactory {
        def apply(
            masterAddress: SimpleSocketAddress,
            masterPrefix: String,
            codeAddress: CodeAddress
        ): CreateNode = new CreateNode {
          def requestOneNewJobFromJobScheduler(
              k: ResourceRequest
          )(implicit taskConfig: TasksConfig) = IO.pure(Left("no"))
        }
      },
      getNodeName = new GetNodeName {
        def getNodeName(config: TasksConfig) = IO.pure(RunningJobId("x"))
      }
    )
    val composite = CompositeElasticSupport(
      List(
        CompositeMember("throwing", throwing, _.startsWith("x")),
        CompositeMember.catchAll("b", fake("b", calls))
      )
    )

    noException should be thrownBy composite.shutdownFromNodeRegistry
      .shutdownRunningNode(RunningJobId("x-1"))
      .unsafeRunSync()
    calls.get.unsafeRunSync() shouldBe Nil
  }

  test("running to pending conversion is answered by the owning member") {
    val calls = recorder
    val composite = CompositeElasticSupport(
      List(
        CompositeMember("a", fake("a", calls), _.startsWith("a-")),
        CompositeMember.catchAll("b", fake("b", calls))
      )
    )

    composite.convertRunningToPending
      .convertRunningToPending(RunningJobId("a-1"))
      .unsafeRunSync() shouldBe Some(PendingJobId("a-1-p"))

    createNodeOf(composite)
      .convertRunningToPending(RunningJobId("b-1"))
      .unsafeRunSync() shouldBe Some(PendingJobId("b-1-p"))

    calls.get.unsafeRunSync() shouldBe List("a:convert:a-1", "b:convert:b-1")
  }

  test("a configured node name is used as is, without asking any member") {
    val calls = recorder
    val composite = CompositeElasticSupport(
      List(
        CompositeMember("a", fake("a", calls), _.startsWith("a-")),
        CompositeMember.catchAll("b", fake("b", calls))
      )
    )

    composite.getNodeName
      .getNodeName(tasksConfig("""tasks.elastic.nodename = "a-42""""))
      .unsafeRunSync() shouldBe RunningJobId("a-42")

    calls.get.unsafeRunSync() shouldBe Nil
  }

  test("without a configured node name the owning member names the node") {
    val calls = recorder
    val composite = CompositeElasticSupport(
      List(
        CompositeMember(
          "a",
          fake("a", calls, spawns("a-node"), "b-node", None),
          _.startsWith("a-")
        ),
        CompositeMember(
          "b",
          fake("b", calls, spawns("b-node"), "b-node", None),
          _.startsWith("b-")
        )
      )
    )

    composite.getNodeName
      .getNodeName(emptyTasksConfig)
      .unsafeRunSync() shouldBe RunningJobId("b-node")

    calls.get.unsafeRunSync() shouldBe List("a:getNodeName", "b:getNodeName")
  }

  test("self shutdown reaches the owning member") {
    val calls = recorder
    val composite = CompositeElasticSupport(
      List(
        CompositeMember("a", fake("a", calls), _.startsWith("a-")),
        CompositeMember("b", fake("b", calls), _.startsWith("b-"))
      )
    )

    val exitCode = Deferred[IO, ExitCode].unsafeRunSync()
    composite.shutdownFromWorker
      .shutdownRunningNode(exitCode, RunningJobId("b-1"))
      .unsafeRunSync()

    calls.get.unsafeRunSync() shouldBe List("b:shutdownSelf:b-1")
    exitCode.tryGet.unsafeRunSync() shouldBe None
  }

  test("self shutdown of an unowned node exits the process") {
    val calls = recorder
    val composite = CompositeElasticSupport(
      List(CompositeMember("a", fake("a", calls), _.startsWith("a-")))
    )

    val exitCode = Deferred[IO, ExitCode].unsafeRunSync()
    composite.shutdownFromWorker
      .shutdownRunningNode(exitCode, RunningJobId("c-1"))
      .unsafeRunSync()

    calls.get.unsafeRunSync() shouldBe Nil
    exitCode.tryGet.unsafeRunSync() shouldBe Some(ExitCode(0))
  }

  test("the host configuration comes from the member owning this node") {
    val calls = recorder
    val local = new LocalConfiguration(1, 2, 3, Nil)
    val composite = CompositeElasticSupport(
      List(
        CompositeMember(
          "a",
          fake("a", calls, spawns("a-node"), "a-node", Some(_ => local)),
          _.startsWith("a-")
        ),
        CompositeMember.catchAll("b", fake("b", calls))
      )
    )

    val hostConfig = composite.hostConfig.getOrElse(fail("expected a value"))

    hostConfig(
      tasksConfig("""tasks.elastic.nodename = "a-42"""")
    ) shouldBe local
  }

  test(
    "the host configuration falls back to the default when the owner has none"
  ) {
    val calls = recorder
    val composite = CompositeElasticSupport(
      List(
        CompositeMember("a", fake("a", calls), _.startsWith("a-")),
        CompositeMember.catchAll("b", fake("b", calls))
      )
    )

    val hostConfig = composite.hostConfig.getOrElse(fail("expected a value"))

    hostConfig(
      tasksConfig(
        """tasks.elastic.nodename = "a-42"
          |tasks.disableRemoting = true""".stripMargin
      )
    ) shouldBe a[LocalConfigurationFromConfig]
  }

  test("a package server is needed if any member needs one") {
    val calls = recorder
    val noServer = new ElasticSupport(
      hostConfig = None,
      shutdownFromNodeRegistry = fake("x", calls).shutdownFromNodeRegistry,
      shutdownFromWorker = fake("x", calls).shutdownFromWorker,
      createNodeFactory = fake("x", calls).createNodeFactory,
      getNodeName = fake("x", calls).getNodeName,
      needsPackageServer = false
    )

    CompositeElasticSupport(
      List(CompositeMember.catchAll("x", noServer))
    ).needsPackageServer shouldBe false

    CompositeElasticSupport(
      List(
        CompositeMember("x", noServer, _.startsWith("x")),
        CompositeMember.catchAll("y", fake("y", calls))
      )
    ).needsPackageServer shouldBe true

    CompositeElasticSupport(
      List(
        CompositeMember("x", noServer, _.startsWith("x")),
        CompositeMember.catchAll("y", fake("y", calls))
      ),
      needsPackageServer = false
    ).needsPackageServer shouldBe false
  }

  test("members must be named uniquely") {
    val calls = recorder
    an[IllegalArgumentException] should be thrownBy CompositeElasticSupport(
      List(
        CompositeMember("a", fake("a", calls), _.startsWith("a-")),
        CompositeMember.catchAll("a", fake("a", calls))
      )
    )
  }

  test("only the last member may be a catch all") {
    val calls = recorder
    an[IllegalArgumentException] should be thrownBy CompositeElasticSupport(
      List(
        CompositeMember.catchAll("a", fake("a", calls)),
        CompositeMember("b", fake("b", calls), _.startsWith("b-"))
      )
    )
    an[IllegalArgumentException] should be thrownBy CompositeElasticSupport(
      List(
        CompositeMember.catchAll("a", fake("a", calls)),
        CompositeMember.catchAll("b", fake("b", calls))
      )
    )
  }

  test("an empty composite is rejected") {
    an[IllegalArgumentException] should be thrownBy CompositeElasticSupport(
      Nil
    )
  }
}

class ProcessConfigNodeIdTestSuite extends FunSuite with Matchers {

  import tasks.elastic.process._

  val config = ShellConfig(
    List(
      ProcessContext("alpha", "localhost"),
      ProcessContext("beta", "localhost")
    )
  )

  test("a process node id is owned by the context which spawned it") {
    config.ownsNodeId("alpha:1234") shouldBe true
    config.ownsNodeId("beta:1234") shouldBe true
  }

  test("a node id of another backend is not owned") {
    config.ownsNodeId("gamma:1234") shouldBe false
    config.ownsNodeId("default/podname") shouldBe false
    config.ownsNodeId("arn:aws:ecs:us-east-1:1:task/cluster/abc") shouldBe false
  }
}
