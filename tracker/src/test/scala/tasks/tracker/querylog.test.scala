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

package tasks.tracker

import org.scalatest._

import org.scalatest.Matchers

import tasks.circesupport._
import com.typesafe.config.ConfigFactory
import scala.concurrent._
import scala.concurrent.duration._

import tasks._

object QueryLogTest extends TestHelpers {

  val task1 = AsyncTask[Input, Int]("task1", 1) {
    input => implicit computationEnvironment =>
      releaseResources
      for {
        _ <- scatter(input)(ResourceRequest(1, 500))
      } yield {
        log.info("ALL DONE")
        1
      }
  }

  val gather = AsyncTask[Input, Int]("gather", 1) {
    input => implicit computationEnvironment =>
      Future(1)
  }
  val work = AsyncTask[Input, Int]("work", 1) {
    input => implicit computationEnvironment =>
      Future(1)
  }

  val scatter = AsyncTask[Input, Int]("scatter", 1) {
    input => implicit computationEnvironment =>
      releaseResources
      for {
        _ <- Future.traverse(1 to 3) { i =>
          work(Input(i))(ResourceRequest(1, 500))
        }
        _ <- gather(Input(0))(ResourceRequest(1, 500))

      } yield 1

  }

  val file = tasks.util.TempFile.createTempFile(".json")

  val testConfig2 = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=1      
      tasks.tracker.fqcn = default
      tasks.tracker.logFile = ${file.getAbsolutePath}
      """
    )
  }

  def run = {
    withTaskSystem(testConfig2) { implicit ts =>
      import scala.concurrent.ExecutionContext.Implicits.global

      val f1 = task1(Input(1))(ResourceRequest(1, 500))
      val future = for {
        t1 <- f1
      } yield t1

      Await.result(future, atMost = 30 seconds)

    }
  }

}

class QueryLogTestSuite extends FunSuite with Matchers {

  test("should create log") {
    QueryLogTest.run.get
    Thread.sleep(1000)
    QueryLogTest.file.length > 0 shouldBe true
    QueryLogTest.file.canRead shouldBe true
    println(QueryLogTest.file)
    tasks.util.openFileInputStream(QueryLogTest.file) { inputStream =>
      val nodes = QueryLog.readNodes(inputStream,
                                     excludeTaskIds = Set.empty,
                                     includeTaskIds = Set.empty)
      nodes.size shouldBe 6

      QueryLog.trees(nodes).size shouldBe 1


      val collapsed = QueryLog.collapseMultiEdges(nodes) { nodes =>
        (nodes.head.resource,
         tasks.shared.ElapsedTimeNanoSeconds(
           nodes.map(_.elapsedTime.toLong).max))
      }
      QueryLog.dot(collapsed) shouldBe """digraph tasks {"work" [label="work(0.0h,1c)"] ;"gather" [label="gather(0.0h,1c)"] ;"scatter" [label="scatter(0.0h,1c)"] ;"task1" [label="task1(0.0h,1c)"] ;"scatter" -> "work" [label= "3x"] ;"scatter" -> "gather"  ;"task1" -> "scatter"  ;"root" -> "task1"  ;}"""

    }

  }

}
