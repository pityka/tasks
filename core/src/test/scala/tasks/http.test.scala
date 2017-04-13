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
import scala.concurrent.duration._
import scala.concurrent._
import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import akka.actor.{Actor, PoisonPill, ActorRef, Props, ActorSystem}
import com.typesafe.config.ConfigFactory

import java.io._
import tasks.kvstore._

import org.scalatest.FunSpec
import org.scalatest.Matchers

import akka.stream._
import akka.actor._
import tasks.queue._
import tasks.caching._
import tasks.caching.kvstore._
import tasks.fileservice._
import tasks.util._
import tasks.shared._
import tasks.elastic._
import tasks.simpletask._
import akka.io.IO

class HttpSpec
    extends TestKit(ActorSystem("testsystem"))
    with ImplicitSender
    with FunSpecLike
    with Matchers
    with BeforeAndAfterAll {
  self: Suite =>

  implicit val mat = ActorMaterializer()
  val as = implicitly[ActorSystem]
  import as.dispatcher
  implicit val sh = new StreamHelper

  val service = system.actorOf(
      Props(new PackageServerActor(new File("build.sbt"))),
      "test-http-service")

  IO(spray.can.Http) ! spray.can.Http.Bind(service, "0.0.0.0", port = 9999)

  override def afterAll {
    Thread.sleep(1500)
    system.shutdown

  }

  describe("http queue") {
    it("content length") {
      // Thread.sleep(10000)
      0 until 100000 foreach { i =>
        (sh.getContentLength(
            akka.http.scaladsl.model.Uri("http://0.0.0.0:9999/build.sbt")))
      }
      Thread.sleep(100000)

    }
  }
}
