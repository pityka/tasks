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

package example

import tasks._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

object PiTasks {
  case class BatchResult(inside: Int, outside: Int)

  case class BatchInput(batchSize: Option[SharedFile], batchId: Option[Int])
      extends SimplePrerequisitive[BatchInput]

  case class PiInput(inside: Option[Int], outside: Option[Int])
      extends SimplePrerequisitive[PiInput]

  case class PiResult(pi: Double)

  val batchCalc = TaskDefinition[BatchInput, BatchResult]("batch") {
    case BatchInput(Some(size), Some(id)) =>
      implicit ctx =>
        log.info("This is worker " + id)
        val sizeInt = scala.io.Source.fromFile(size.file).mkString.toInt
        val (in, out) = (0 until sizeInt).foldLeft((0, 0)) {
          case ((countIn, countOut), _) =>
            val x = scala.util.Random.nextDouble
            val y = scala.util.Random.nextDouble
            val inside = math.sqrt(x * x + y * y) <= 1.0
            if (inside) (countIn + 1, countOut) else (countIn, countOut + 1)
        }
        BatchResult(in, out)
  }

  val piCalc = TaskDefinition[PiInput, PiResult]("reduce") {
    case PiInput(Some(in), Some(out)) =>
      implicit ctx =>
        PiResult(in.toDouble / (in + out) * 4d)
  }
}

object Fib {

  import scala.concurrent.ExecutionContext.Implicits.global

  case class FibInput(n: Option[Int], tag: Option[List[Boolean]])
      extends SimplePrerequisitive[FibInput]
  object FibInput {
    def apply(n: Int): FibInput = FibInput(Some(n), tag = Some(Nil))
  }

  case class FibOut(n: Int)

  val fibtask: TaskDefinition[FibInput, FibOut] =
    TaskDefinition[FibInput, FibOut]("fib") {

      case FibInput(Some(n), Some(tag)) =>
        implicit ce =>
          n match {
            case 0 => FibOut(0)
            case 1 => FibOut(1)
            case n => {
              val f1 = fibtask(FibInput(Some(n - 1), Some(false :: tag)))(
                  CPUMemoryRequest(1, 1)).?
              val f2 = fibtask(FibInput(Some(n - 2), Some(true :: tag)))(
                  CPUMemoryRequest(1, 1)).?
              val f3 = for {
                r1 <- f1
                r2 <- f2
              } yield FibOut(r1.n + r2.n)
              tasks.LauncherActor.block(CPUMemoryRequest(1, 100)) {
                Await.result(f3, atMost = 500 seconds)
              }
            }

          }

    }

}

object PiApp extends App {

  import PiTasks._
  import Fib._

  withTaskSystem { implicit ts =>
    val numTasks = 100

    val taskSize: SharedFile = {
      val tmp = java.io.File.createTempFile("size", ".txt")
      val writer = new java.io.FileWriter(tmp)
      writer.write("1000")
      writer.close
      SharedFile(tmp, name = "taskSize.txt")
    }

    val pi: Future[PiResult] =
      Future
        .sequence(1 to numTasks map { i =>
          batchCalc(BatchInput(Some(taskSize), Some(i)))(
              CPUMemoryRequest(1, 50)).?
        })
        .flatMap { batches =>
          piCalc(PiInput(Some(batches.map(_.inside).sum),
                         Some(batches.map(_.outside).sum)))(
              CPUMemoryRequest(1, 50)).?
        }

    val fibResult = fibtask(FibInput(16))(CPUMemoryRequest(1, 50)).?

    println(Await.result(pi, atMost = 10 minutes))
    println(Await.result(fibResult, atMost = 10 minutes))

  }

}
