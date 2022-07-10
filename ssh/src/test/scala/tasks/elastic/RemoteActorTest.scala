/*
 * The MIT License
 *
 * Copyright (c) 2018 Istvan Bartha
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

package tasks.elastic

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}

import org.scalatest.matchers.should.Matchers

import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

import scala.concurrent.Future
import tasks._
import com.typesafe.config.ConfigFactory
import tasks.util.SerializedActorRef
import scala.concurrent.duration._
import akka.event.Logging
import akka.pattern.ask

object SHRemoteActorTest extends TestHelpers {
  import akka.actor._
  class Actor1(n: Int) extends Actor {
    val ar = Array.ofDim[Boolean](n)
    0 until n foreach (i => ar(i) = false)
    val refs = Array.ofDim[ActorRef](n)
    val log = Logging(context.system, this)

    def receive = { case x: Int =>
      log.info("received: " + x)
      ar(x) = true
      refs(x) = sender()
      if (ar.forall(identity)) {
        log.info("Unblock")
        refs.foreach(_ ! "unblock")
      } else {
        log.info(
          s"Waiting for ${ar.toList.zipWithIndex.filter(b => !b._1).map(_._2)}"
        )
      }
    }
  }

  case class NumInput(num: Int)

  object NumInput {
    implicit val codec: JsonValueCodec[NumInput] = JsonCodecMaker.make

  }
  case class ActorInput(n: Int, actor: SerializedActorRef)

  object ActorInput {
    implicit val codec: JsonValueCodec[ActorInput] = JsonCodecMaker.make
  }

  implicit val codec: JsonValueCodec[Seq[String]] = JsonCodecMaker.make

  val outerTask =
    AsyncTask[NumInput, Seq[String]]("remoteactortest_outertask", 1) {
      case NumInput(num) =>
        implicit ce =>
          ce.log.info("run outer")
          implicit val as = ce.actorSystem
          val ac1 = as.actorOf(Props(new Actor1(num)), "1")

          Future
            .sequence(0 until num map { n =>
              innerTask(ActorInput(n, SerializedActorRef(ac1)))(
                ResourceRequest(1, 500),
                noCache = true
              )
            })

    }
  val innerTask =
    AsyncTask[ActorInput, String]("remoteactortest_innertask", 1) {
      case ActorInput(n, actor) =>
        implicit ce =>
          ce.log.info("run inner " + n)
          implicit val as = ce.actorSystem
          actor.resolve(60 seconds).flatMap { ac =>
            ce.log.info("sending my number " + n)
            ac.ask(n)(akka.util.Timeout(120 seconds)).mapTo[String]
          }
    }

  def run = {
    // import scala.concurrent.ExecutionContext.Implicits.global

    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    println(tmp)
    val testConfig2 =
      ConfigFactory.parseString(
        s"""tasks.fileservice.disableOnSlave = true
        akka.loglevel= INFO
        tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=1
      tasks.elastic.maxNodes = 10
      tasks.elastic.engine = "tasks.elastic.sh.SHElasticSupport"
      tasks.elastic.queueCheckInterval = 1 seconds  
      tasks.addShutdownHook = false
      tasks.failuredetector.acceptable-heartbeat-pause = 5 s
      tasks.worker-main-class = "tasks.TestSlave"
      tasks.elastic.sh.workdir = ${tmp.getAbsolutePath}
      tasks.elastic.javaCommandLine = "-Dtasks.fileservice.disableOnSlave=true"
      """
      )

    withTaskSystem(testConfig2.withFallback(testConfig)) { implicit ts =>
      val f1 = outerTask(NumInput(5))(ResourceRequest(1, 50), noCache = true)

      await(f1)

    }
  }

}

class SHWithRemoteActorTestSuite extends FunSuite with Matchers {

  test("test") {
    assert(
      SHRemoteActorTest.run.get == List(
        "unblock",
        "unblock",
        "unblock",
        "unblock",
        "unblock"
      )
    )

  }

}
