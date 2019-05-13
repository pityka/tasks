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

package tasks.ui

import org.scalatest._

import org.scalatest.Matchers

import tasks.circesupport._
import com.typesafe.config.ConfigFactory
import scala.concurrent._
import scala.concurrent.duration._

import tasks._

object UIBackendTest extends TestHelpers {

  val testTask = AsyncTask[Input, Int]("nodeallocationtest", 1) {
    _ => implicit computationEnvironment =>
      log.info("Hello from task")
      Thread.sleep(1000)
      Future(1)
  }

  val testConfig2 = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=1      
      tasks.ui.fqcn = default
      """
    )
  }

  val websocketContents = scala.collection.mutable.ArrayBuffer[UIQueueState]()

  def run = {
    withTaskSystem(testConfig2) { implicit ts =>
      import scala.concurrent.ExecutionContext.Implicits.global

      val sf =
        Await.result(
          SharedFile(akka.stream.scaladsl.Source.single(akka.util.ByteString()),
                     "boo"),
          atMost = 50 seconds)

      val f1 = testTask(Input(1, sf))(ResourceRequest(1, 500))
      val f2 = testTask(Input(2, sf))(ResourceRequest(1, 500))
      val f3 = testTask(Input(3, sf))(ResourceRequest(1, 500))
      val future = for {
        t1 <- f1
        t2 <- f2
        t3 <- f3
      } yield t1 + t2 + t3

      WebSocketClient.make("ws://localhost:28880/states")(frame =>
        synchronized {
          websocketContents += io.circe.parser
            .decode[UIQueueState](frame)
            .right
            .get
      })

      Await.result(future, atMost = 30 seconds)

    }
  }

}

class UIBackendTestSuite extends FunSuite with Matchers {

  test("ui backend should serve /states") {
    UIBackendTest.run.get
    UIBackendTest.websocketContents.nonEmpty shouldBe true

  }

}
