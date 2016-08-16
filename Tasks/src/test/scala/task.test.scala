/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Copyright (c) 2016 Istvan Bartha
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
import org.scalatest._
import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import akka.testkit.EventFilter
import scala.concurrent.duration._

import scala.io.Source
import akka.actor.{Actor, PoisonPill, ActorRef, Props, ActorSystem}
import akka.actor.Actor._
import scala.concurrent.Future
import com.typesafe.config.ConfigFactory

import tasks.queue._
import tasks.caching._
import tasks.caching.kvstore._
import tasks.fileservice._
import tasks.util._
import tasks.shared._
import tasks.simpletask._
import tasks.deploy._

object TestConf {
  val str = """my-pinned-dispatcher {
  executor = "thread-pool-executor"
  type = PinnedDispatcher
  thread-pool-executor.allow-core-timeout=off
}
akka.loglevel = "ERROR"
akka.event-handlers = ["akka.testkit.TestEventListener"] """
}

class TaskTestSuite
    extends TestKit(
        ActorSystem("testsystem",
                    ConfigFactory
                      .parseString(TestConf.str)
                      .withFallback(ConfigFactory.load("akkaoverrides.conf"))))
    with ImplicitSender
    with FunSuiteLike
    with BeforeAndAfterAll {
  // remote.start("localhost", 3985) //Start the server
  // remote.register("cache-service", )

  implicit val prefix = FileServicePrefix(Vector())

  val tmpfile = TempFile.createTempFile(".cache")
  tmpfile.delete

  val folder = new FolderFileStorage(
      new java.io.File(getClass.getResource("/").getPath),
      true)

  val fileActor = system.actorOf(
      Props(new FileService(folder)).withDispatcher("my-pinned-dispatcher"),
      "queue")

  val cacher = system.actorOf(
      Props(
          new TaskResultCache(
              LevelDBCache(tmpfile,
                           akka.serialization.SerializationExtension(system)),
              FileServiceActor(fileActor)))
        .withDispatcher("my-pinned-dispatcher"),
      "cache")

  val balancer = system.actorOf(
      Props[TaskQueue].withDispatcher("my-pinned-dispatcher"),
      "fileservice")
  val nlc = system.actorOf(
      Props[NodeLocalCache].withDispatcher("my-pinned-dispatcher"),
      name = "nodeLocalCache")
  val starter1 = system.actorOf(
      Props(
          new TaskLauncher(balancer,
                           nlc,
                           CPUMemoryAvailable(1, 100000),
                           1 milliseconds))
        .withDispatcher("my-pinned-dispatcher"),
      "launcher1")
  val starter2 = system.actorOf(
      Props(
          new TaskLauncher(balancer,
                           nlc,
                           CPUMemoryAvailable(1, 100000),
                           1 milliseconds))
        .withDispatcher("my-pinned-dispatcher"),
      "launcher2")

  implicit val starter = QueueActor(balancer)

  implicit val fileService = FileServiceActor(fileActor)

  implicit val cache = CacheActor(cacher)

  override def afterAll {
    Thread.sleep(1500)
    system.shutdown

  }

  test("Trivial") {
    val st1 = system.actorOf(Props(new SimpleTask(1, 32324)))
    st1 ! GetBackResult
    expectMsg(3000 millis, IntResult(1))
  }

  test("Simple test") {

    val st1 = system.actorOf(Props(new SimpleTask(1, 123)))

    val st2 = system.actorOf(Props(new SimpleTask(0)))

    st2 ! GetBackResult
    expectNoMsg(6000 millis)
    info("Task computation should not start before dependency is done.")
    st1 ! GetBackResult

    st1 ! AddTarget(st2, SimpleTask.testUpdater)
    expectResult(Seq(IntResult(1), IntResult(1)))(
        receiveN(2, 600 millis).toSeq)
    info("target added + end result received")

    st2 ! GetBackResult
    expectMsg(10 millis, IntResult(1))
    info("On request, end result received again, without computation.")

  }

  test("Chain of 3") {
    val st1 = system.actorOf(Props(new SimpleTask(15, 645)))
    val st2 = system.actorOf(Props(new SimpleTask(0)))
    val st3 = system.actorOf(Props(new SimpleTask(0)))

    st1 ! AddTarget(st2, SimpleTask.testUpdater)
    st2 ! AddTarget(st3, SimpleTask.testUpdater)

    st3 ! GetBackResult
    expectMsg(600 millis, IntResult(15))

  }

  test("Parallel N") {
    val n = 500
    val st1 = system.actorOf(Props(new SimpleTask(16, 6345)))
    val st2 = system.actorOf(Props(new SimpleTask(0, 63451)))
    val childs = (1 to n) map (x =>
            system.actorOf(Props(new SimpleTask(0, x))))

    st1 ! AddTarget(st2, SimpleTask.testUpdater)

    childs.foreach { x =>
      st2 ! AddTarget(x, SimpleTask.testUpdater)
    }

    childs.foreach { x =>
      x ! GetBackResult
    }

    receiveN(n, 25000 millis).toList.foreach { result =>
      expectResult(IntResult(16))(result)
    }

  }

  test(
      "3 parallel task (50ms each) should finish on 3 TaskLaunchers in 150ms.") {
    val starter3 =
      system.actorOf(Props(
                         new TaskLauncher(balancer,
                                          nlc,
                                          CPUMemoryAvailable(1, 100000),
                                          1 milliseconds))
                       .withDispatcher("my-pinned-dispatcher"),
                     "launcher3")

    // This is to "warm up" the new launcher. dont' exactly know why, but the first task is slow.
    val st12 = system.actorOf(Props(new SimpleTask(12)))
    val st13 = system.actorOf(Props(new SimpleTask(13)))
    val st14 = system.actorOf(Props(new SimpleTask(14)))

    st12 ! GetBackResult
    st13 ! GetBackResult
    st14 ! GetBackResult

    receiveN(3, 3500 millis)

    st12 ! PoisonPill
    st13 ! PoisonPill
    st14 ! PoisonPill

    // Wait for the launcher to fully load
    expectNoMsg(3000 millis)

    val st1 = system.actorOf(Props(new SimpleTask(2)))
    val st2 = system.actorOf(Props(new SimpleTask(3)))
    val st3 = system.actorOf(Props(new SimpleTask(4)))

    st1 ! GetBackResult
    st2 ! GetBackResult
    st3 ! GetBackResult

    val result = receiveN(3, 550 millis)

    starter3 ! PoisonPill

    expectResult(9)(
        result.map(_.asInstanceOf[IntResult].value).foldLeft[Int](0)(_ + _))
  }

  test("3 parallel task (50ms each) finish on 2 TaskLaunchers in 200ms.") {
    val st1 = system.actorOf(Props(new SimpleTask(2, 111)))
    val st2 = system.actorOf(Props(new SimpleTask(3, 111)))
    val st3 = system.actorOf(Props(new SimpleTask(4, 111)))

    st1 ! GetBackResult
    st2 ! GetBackResult
    st3 ! GetBackResult

    receiveN(3, 200 millis)

    // expectResult(9)(receiveN(3, 100 millis).map(_.asInstanceOf[IntResult].value).foldLeft[Int](0)(_ + _))
  }

  test("Already done task should be served from cache.") {
    val st1 = system.actorOf(Props(new SimpleTask(23, 992)))
    st1 ! GetBackResult
    expectMsg(390 millis, IntResult(23))

    val st2 = system.actorOf(Props(new SimpleTask(23, 993)))
    st2 ! GetBackResult
    expectMsg(195 millis, IntResult(23))
    val st3 = system.actorOf(Props(new SimpleTask(23, 994)))
    st3 ! GetBackResult
    expectMsg(195 millis, IntResult(23))

  }

  test("Failed task.") {
    val st1 = system.actorOf(Props(new SimpleTask(42)))
    st1 ! GetBackResult
    expectMsgPF(3000 millis) {
      case akka.actor.Status.Failure(cause)
          if cause.getMessage == "failtest" =>
        true
    }

    try {
      EventFilter[RuntimeException](occurrences = 1) intercept {
        throw new RuntimeException("ASDF")
      }
    } catch {
      case x: RuntimeException if x.getMessage == "ASDF" =>
        expectResult(true)(true)
    }
  }

}
