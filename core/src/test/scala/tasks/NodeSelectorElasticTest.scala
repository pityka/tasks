/*
 * The MIT License
 *
 * Copyright (c) 2026 Istvan Bartha
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package tasks

import scala.concurrent.duration._

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers
import org.ekrich.config.ConfigFactory

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global

import tasks.jsonitersupport._
import tasks.JvmElasticSupport.JvmGrid
import tasks.shared.NodeSelector

object NodeSelectorElasticTest extends TestHelpers {

  // Returns the launcher (worker) address that ran the task, so we can tell
  // workers apart in the assertions below.
  val whichLauncher =
    Task[Input, String]("whichLauncher", 1) { _ => implicit ce =>
      IO.pure(launcherName.name)
    }

  /** Map any Has(label) clauses inside a selector to a label set. Used by the
    * JvmGrid spawn strategy to materialise workers that exactly satisfy the
    * affinity portion of the request. Negative clauses (Not, etc.) contribute
    * nothing.
    */
  def labelsFromSelector(sel: NodeSelector): Set[String] = sel match {
    case NodeSelector.Always      => Set.empty
    case NodeSelector.Has(label)  => Set(label)
    case NodeSelector.Not(_)      => Set.empty
    case NodeSelector.And(xs)     => xs.iterator.flatMap(labelsFromSelector).toSet
    case NodeSelector.Or(xs)      => xs.iterator.flatMap(labelsFromSelector).toSet
  }

  def labelsForRequest(req: tasks.shared.ResourceRequest): Set[String] =
    req.nodeSelector.fold(Set.empty[String])(labelsFromSelector)

  def baseConfig(maxNodes: Int) = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      // hosts.numCPU = 0 forces every task onto an elastically-spawned worker
      // — the master node itself cannot run anything.
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
         |hosts.numCPU = 0
         |tasks.disableRemoting = false
         |tasks.addShutdownHook = false
         |tasks.elastic.maxNodes = $maxNodes
         |tasks.elastic.maxNodesCumulative = ${maxNodes * 4}
         |tasks.failuredetector.acceptable-heartbeat-pause = 10 s
         |""".stripMargin
    )
  }
}

class NodeSelectorElasticTestSuite extends FunSuite with Matchers {
  import NodeSelectorElasticTest._

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Info))
    .replace()

  test(
    "elastic workers advertise labels and the master only schedules matching tasks"
  ) {
    val program = withTaskSystem(
      baseConfig(maxNodes = 4),
      Resource.pure(None),
      JvmGrid
        .make(externalQueueState = None, labelsForRequest = labelsForRequest)
        .map(v => Some(v._2))
    ) { implicit ts =>
      // 1. Has(zone:a) — JvmGrid spawns a worker labeled "zone:a".
      val a1: IO[String] = whichLauncher(Input(1))(
        ResourceRequest(cpu = 1, memory = 100, NodeSelector.Has("zone:a"))
      )

      // 2. Has(zone:b) — the existing zone:a worker can't satisfy this; the
      //    grid spawns a SECOND worker, this one labeled "zone:b".
      val b1: IO[String] = whichLauncher(Input(2))(
        ResourceRequest(cpu = 1, memory = 100, NodeSelector.Has("zone:b"))
      )

      // 3. Has(zone:a) again — should be picked up by the zone:a worker from
      //    step 1 (workers are reused when they fit), proving the master's
      //    view of labels survives the lifetime of the worker.
      val a2: IO[String] = whichLauncher(Input(3))(
        ResourceRequest(cpu = 1, memory = 100, NodeSelector.Has("zone:a"))
      )

      // 4. Avoidance: Not(Has(zone:b)) — only the zone:a worker has an empty
      //    intersection with {zone:b}. The master must pick that worker (or
      //    spawn an unlabeled one); either way, NOT the zone:b worker.
      val avoid: IO[String] = whichLauncher(Input(4))(
        ResourceRequest(
          cpu = 1,
          memory = 100,
          NodeSelector.Not(NodeSelector.Has("zone:b"))
        )
      )

      for {
        a1n <- a1
        b1n <- b1
        a2n <- a2
        avoidN <- avoid
      } yield (a1n, b1n, a2n, avoidN)
    }.timeout(60.seconds)

    val (a1n, b1n, a2n, avoidN) = program.unsafeRunSync().toOption.get

    // Same affinity reuses the same worker.
    a1n shouldBe a2n
    // Different affinities land on different workers.
    a1n should not be b1n
    // Avoidance keeps the task off the disallowed worker.
    avoidN should not be b1n
  }

  test(
    "task with an unmatched affinity selector does not run within the timeout"
  ) {
    // labelsForRequest returns Set.empty for selectors with no Has() clause,
    // so a Has(missing) selector spawns an unlabeled worker that still cannot
    // satisfy the request → the scheduler keeps looking forever.
    val program = withTaskSystem(
      baseConfig(maxNodes = 1),
      Resource.pure(None),
      JvmGrid
        .make(
          externalQueueState = None,
          // intentionally always-empty: every spawned worker is unlabeled.
          labelsForRequest = _ => Set.empty
        )
        .map(v => Some(v._2))
    ) { implicit ts =>
      whichLauncher(Input(99))(
        ResourceRequest(cpu = 1, memory = 100, NodeSelector.Has("nonexistent"))
      ).timeout(5.seconds).attempt.map(_.toOption)
    }

    program.unsafeRunSync().toOption.get shouldBe None
  }
}
