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

package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should._
import org.scalatest._
import org.ekrich.config.ConfigFactory

import tasks.jsonitersupport._

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import tasks.releaseResourcesEarly
import tasks.elastic.process.LocalShellElasticSupport
import cats.effect.kernel.Resource

object RemoteSpawnTaskTest {

  def serial(n: Int): Int = n match {
    case 0 => 0
    case 1 => 1
    case _ => serial(n - 1) + serial(n - 2)
  }

  case class FibInput(n: Option[Int], tag: Option[List[Boolean]])

  object FibInput {
    implicit val codec: JsonValueCodec[FibInput] = JsonCodecMaker.make

    def apply(n: Int): FibInput = FibInput(Some(n), tag = Some(Nil))
  }

  case class FibOut(n: Int)
  object FibOut {
    implicit val codec: JsonValueCodec[FibOut] = JsonCodecMaker.make

  }

  val parent: TaskDefinition[FibInput, FibOut] =
    Task[FibInput, FibOut]("parent", 1) {

      case FibInput(Some(n), Some(tag)) => { implicit ce =>
        val prg = n match {
          case 0 => IO.pure(FibOut(0))
          case 1 => IO.pure(FibOut(1))
          case n => {
            val f1 = child(FibInput(Some(n - 1), Some(false :: tag)))(
              ResourceRequest(cpu=(1,1),memory=1,gpu=1,scratch=0)
            )
            val f2 = child(FibInput(Some(n - 2), Some(true :: tag)))(
              ResourceRequest(cpu=(1,1),memory=1,gpu=1,scratch=0)
            )
            for {
              _ <- releaseResourcesEarly
              r <- IO.both(f1, f2)

            } yield {
              // scribe.warn(("AAA!!!",r,ce.resourceAllocated).toString)
              FibOut(r._1.n + r._2.n)
            }
          }

        }

        prg
      }

      case _ => ???

    }
  val child: TaskDefinition[FibInput, FibOut] =
    Task[FibInput, FibOut]("child", 1) {

      case FibInput(Some(n), Some(tag)) => { implicit ce =>
        val prg = n match {
          case 0 => IO.pure(FibOut(0))
          case 1 => IO.pure(FibOut(1))
          case n => IO.pure(FibOut(3))

        }

        prg
      }

      case _ => ???

    }

}

class RemoteSpawnTaskTestSuite
    extends FunSuite
    with Matchers
    with BeforeAndAfterAll
    with TestHelpers {

       scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Trace), outputFormat = scribe.output.format.ASCIIOutputFormat)
    
    .replace()

    // scribe.debug("BOOO")

    

   val testConfig2 = {    
        val tmp = tasks.util.TempFile.createTempFile(".temp")
        tmp.delete()

      s"""
      
tasks.cache.enabled = false
tasks.disableRemoting = false
hosts.numCPU=0
hosts.gpus = []
tasks.fileservice.storageURI=${tmp.getAbsolutePath}
tasks.worker-main-class = "tasks.TestWorker"
tasks.elastic.logQueueStatus = false
tasks.sh.contexts = [
  {
  context = c1 
  hostname = localhost
  gpu = [0,1,2,3]
  cpu = 4
  memory = 1000
  }  
]
      """
    
  }
import scala.sys.process._
    println("pwd".!!)
  val pair = defaultTaskSystem(testConfig2,
      Resource.pure(None),
      Resource
        .eval(LocalShellElasticSupport.make(Some(ConfigFactory.parseString(testConfig2))))
        .map(Some(_))).allocated.unsafeRunSync()
  implicit val system: TaskSystemComponents = pair._1._1
  import RemoteSpawnTaskTest._

  test("task from parent on remote node") {
    val n = 4
    val r = (parent(FibInput(n))(ResourceRequest(cpu=(1,1),memory=1,gpu=1,scratch=0))).unsafeRunSync().n
    assertResult(r)(6)
  }

  override def afterAll() = {
    Thread.sleep(1500)
    pair._2.unsafeRunSync()

  }
}
