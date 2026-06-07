package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers
import cats.effect.IO
import cats.effect.kernel.{Deferred, Ref, Resource}
import cats.effect.unsafe.implicits.global
import cats.effect.ExitCode

import org.ekrich.config.ConfigFactory

import tasks.jsonitersupport._
import tasks.elastic._
import tasks.shared._
import tasks.util.config.TasksConfig
import tasks.util.Transaction
import tasks.queue.QueueImpl

import scala.concurrent.duration._

/** Regression test for the request-batch-size bug:
  *
  * `handleQueueStat` derives the list of node requests to issue from
  * `neededNodes: Map[ResourceRequest, Int]`, but historically did
  * `neededNodes.take(allowedNewNodes)` (which takes that many *map entries*
  * = distinct resource shapes) and then iterated over the entries dropping
  * the count via `case (req, _) => ...`. So for the common case of many
  * tasks of the same shape, exactly **one** node was pre-committed and
  * one AWS submit issued per `handleQueueStat` invocation, regardless of
  * `maxNodes`. Combined with the queue mutex (which holds across the
  * AWS submit), this produced "scale-up goes one node at a time."
  *
  * This test sets `maxNodes = 5`, submits many tasks of the same shape,
  * and uses a `CreateNode` whose request handler sleeps for many seconds
  * before returning `Right`. After a short delay it asserts that the
  * occupied capacity (`inFlightRequests + pending`) has reached `maxNodes`.
  *
  * With the bug, the observed occupied capacity is 1.
  * With the fix, it should equal `maxNodes`.
  */
object RequestNodeBatchSizeTest extends TestHelpers {

  val MaxNodes = 5

  val sleepingTask =
    Task[Input, Int]("requestNodeBatchSizeTask", 1) { _ => _ =>
      IO.sleep(60.seconds).as(0)
    }

  class SlowCreateNode extends CreateNode {
    val callCount = new java.util.concurrent.atomic.AtomicInteger(0)
    def requestOneNewJobFromJobScheduler(
        requestSize: ResourceRequest
    )(implicit config: TasksConfig)
        : IO[Either[String, (PendingJobId, ResourceAvailable)]] = {
      callCount.incrementAndGet()
      IO.sleep(10.seconds) *>
        IO.pure(
          Right(
            (
              PendingJobId(java.util.UUID.randomUUID.toString),
              ResourceAvailable(
                cpu = requestSize.cpu._2,
                memory = requestSize.memory,
                scratch = requestSize.scratch,
                gpu = (0 until requestSize.gpu).toList,
                image = requestSize.image
              )
            )
          )
        )
    }
  }

  class NoOpShutdown extends ShutdownNode with ShutdownSelfNode {
    def shutdownRunningNode(nodeName: RunningJobId): IO[Unit] = IO.unit
    def shutdownRunningNode(
        exitCode: Deferred[IO, ExitCode],
        nodeName: RunningJobId
    ): IO[Unit] = IO.unit
    def shutdownPendingNode(nodeName: PendingJobId): IO[Unit] = IO.unit
  }

  class SlowCreateNodeFactory(node: SlowCreateNode) extends CreateNodeFactory {
    def apply(
        master: tasks.util.SimpleSocketAddress,
        masterPrefix: String,
        codeAddress: CodeAddress
    ) = node
  }

  object GetNodeNameNoOp extends GetNodeName {
    def getNodeName(config: TasksConfig) = IO.pure(RunningJobId(config.nodeName))
  }

  def elasticSupport(
      node: SlowCreateNode
  ): Resource[IO, Option[ElasticSupport]] =
    Resource.pure(
      Some(
        new ElasticSupport(
          hostConfig = None,
          shutdownFromNodeRegistry = new NoOpShutdown,
          shutdownFromWorker = new NoOpShutdown,
          createNodeFactory = new SlowCreateNodeFactory(node),
          getNodeName = GetNodeNameNoOp
        )
      )
    )

  val testConfig2 = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU = 0
      tasks.addShutdownHook = false
      tasks.elastic.maxNodes = $MaxNodes
      tasks.elastic.maxNodesCumulative = 1000
      tasks.elastic.pendingNodeTimeout = 5 minutes
      tasks.disableRemoting = false
      """
    )
  }

  /** Returns (occupied = inFlight + pending) after letting the queue settle for
    * `observeAfter`, before any AWS submit has had a chance to resolve.
    */
  def run(observeAfter: FiniteDuration): IO[Int] = {
    Ref.of[IO, QueueImpl.State](QueueImpl.State.empty).flatMap { stateRef =>
      val node = new SlowCreateNode
      withTaskSystem(
        Some(testConfig2),
        Resource.pure(None),
        elasticSupport(node),
        Resource.pure(Some(Transaction.fromRef(stateRef)))
      ) { implicit ts =>
        import cats.syntax.all._
        val n = 50
        val submit = (1 to n).toList.parTraverse { i =>
          sleepingTask(Input(i))(
            tasks.ResourceRequest(
              cpu = (1, 1),
              memory = 1,
              scratch = 0,
              gpu = 0
            )
          ).attempt.void
        }.start

        for {
          fiber <- submit
          _ <- IO.sleep(observeAfter)
          st <- stateRef.get
          occupied = st.nodes.inFlightRequests.size + st.nodes.pending.size
          _ <- fiber.cancel
        } yield occupied
      }.map(_.toOption.get)
    }
  }

}

class RequestNodeBatchSizeTestSuite extends FunSuite with Matchers {

  test(
    "handleQueueStat pre-commits up to maxNodes per invocation when many same-shape tasks are queued"
  ) {
    val occupied =
      RequestNodeBatchSizeTest.run(500.millis).unsafeRunSync()
    occupied shouldBe RequestNodeBatchSizeTest.MaxNodes
  }

}
