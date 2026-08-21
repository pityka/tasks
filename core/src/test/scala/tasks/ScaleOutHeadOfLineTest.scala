package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers

import org.ekrich.config.ConfigFactory

import tasks.elastic.SimpleDecideNewNode
import tasks.shared._
import tasks.util.message.QueueStat

class ScaleOutHeadOfLineTest extends FunSuite with Matchers {

  private val cv = CodeVersion("test")

  private implicit val config: tasks.util.config.TasksConfig =
    tasks.util.config.parse(() =>
      tasks.util.loadConfig(
        Some(ConfigFactory.parseString("tasks.elastic.maxPending = 10"))
      )
    )

  private val decider = new SimpleDecideNewNode(cv)

  private def decide(
      queued: List[(String, ResourceRequest)],
      registeredNodes: Seq[ResourceAvailable],
      pendingNodes: Seq[ResourceAvailable]
  ) =
    decider.needNewNode(
      QueueStat(
        queued = queued.map { case (id, r) =>
          (id, VersionedResourceRequest(cv, r))
        },
        running = Nil
      ),
      registeredNodes = registeredNodes,
      pendingNodes = pendingNodes
    )

  test(
    "a pending node is claimed by the largest request it can serve, so a smaller queued request still triggers a scale out regardless of queue order"
  ) {
    val big = ResourceRequest((1, 1), 64000, 0, 8, None)
    val small = ResourceRequest((1, 1), 8000, 0, 0, None)

    val pendingBigNode = ResourceAvailable(
      cpu = 16,
      memory = 64000,
      scratch = 0,
      gpu = (0 until 8).toList,
      image = None
    )

    val bigFirst =
      decide(List("big" -> big, "small" -> small), Nil, Seq(pendingBigNode))
    val smallFirst =
      decide(List("small" -> small, "big" -> big), Nil, Seq(pendingBigNode))

    bigFirst shouldBe Map(small -> 1)
    smallFirst shouldBe Map(small -> 1)
  }

  test(
    "a request that fits several nodes takes the smallest of them, leaving the larger node for a request only it can serve"
  ) {
    val wideRequest = ResourceRequest((8, 8), 1000, 0, 0, None)
    val deepRequest = ResourceRequest((1, 1), 64000, 0, 0, None)

    val bigNode = ResourceAvailable(
      cpu = 16,
      memory = 64000,
      scratch = 0,
      gpu = Nil,
      image = None
    )
    val smallNode = ResourceAvailable(
      cpu = 8,
      memory = 1000,
      scratch = 0,
      gpu = Nil,
      image = None
    )

    decide(
      List("wide" -> wideRequest, "deep" -> deepRequest),
      Seq(bigNode, smallNode),
      Nil
    ) shouldBe empty
  }

}
