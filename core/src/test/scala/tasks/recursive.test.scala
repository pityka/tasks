/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Modified work, Copyright (c) 2016 Istvan Bartha

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
import tasks._
import tasks.util.concurrent.await
import tasks.simpletask._
import akka.actor.{Actor, PoisonPill, ActorRef, Props, ActorSystem}
import akka.actor.Actor._
import scala.concurrent._
import duration._
import Duration._
import com.typesafe.config.ConfigFactory
import scala.concurrent.ExecutionContext.Implicits.global

import tasks.queue._
import tasks.caching._
import tasks.fileservice._
import tasks.util._
import tasks.deploy._

object Fib {

  import scala.concurrent.ExecutionContext.Implicits.global

  def serial(n: Int): Int = n match {
    case 0 => 0
    case 1 => 1
    case x => serial(n - 1) + serial(n - 2)
  }

  case class FibInput(n: Option[Int], tag: Option[List[Boolean]])

  object FibInput {
    def apply(n: Int): FibInput = FibInput(Some(n), tag = Some(Nil))
  }

  case class FibOut(n: Int)

  val fibtask: TaskDefinition[FibInput, FibOut] =
    AsyncTask[FibInput, FibOut]("fib", 1) {

      case FibInput(Some(n), Some(tag)) =>
        implicit ce =>
          n match {
            case 0 => Future.successful(FibOut(0))
            case 1 => Future.successful(FibOut(1))
            case n => {
              val f1 = fibtask(FibInput(Some(n - 1), Some(false :: tag)))(
                  CPUMemoryRequest(1, 1))
              val f2 = fibtask(FibInput(Some(n - 2), Some(true :: tag)))(
                  CPUMemoryRequest(1, 1))
              releaseResources
              for {
                r1 <- f1
                r2 <- f2
              } yield FibOut(r1.n + r2.n)
            }

          }

    }

  println(fibtask.taskId)

}

class RecursiveTestSuite
    extends FunSuite
    with Matchers
    with BeforeAndAfterAll {
  val string = """
akka.loglevel = "INFO"
tasks.cacheEnabled = false
tasks.disableRemoting = true

"""

  implicit val system: TaskSystem = customTaskSystem(
      new LocalConfiguration(4, 1000),
      ConfigFactory.parseString(string))
  import Fib._

  test("long") {
    val n = 16
    val r = await(fibtask(FibInput(n))(CPUMemoryRequest(1, 1))).n
    expectResult(r)(serial(n))
  }

  override def afterAll {
    Thread.sleep(1500)
    system.shutdown

  }
}
