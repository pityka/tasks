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
  case class BatchResult(inside: Int, outside: Int) extends Result

  case class BatchInput(batchSize: Option[SharedFile], batchId: Option[Int])
      extends SimplePrerequisitive[BatchInput]

  case class PiInput(inside: Option[Int], outside: Option[Int])
      extends SimplePrerequisitive[PiInput]

  case class PiResult(pi: Double) extends Result

  implicit val update: UpdatePrerequisitive[PiInput, BatchResult] =
    UpdatePrerequisitive {
      case (old, i: BatchResult) =>
        old.copy(inside = Some(i.inside + old.inside.getOrElse(0)),
                 outside = Some(i.outside + old.outside.getOrElse(0)))
    }

  val batchCalc = TaskDefinition[BatchInput, BatchResult] {
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

  val piCalc = TaskDefinition[PiInput, PiResult] {
    case PiInput(Some(in), Some(out)) =>
      implicit ctx =>
        PiResult(in.toDouble / (in + out) * 4d)
  }
}

object PiApp extends App {

  import PiTasks._

  withTaskSystem { implicit ts =>
    val numTasks = 100
    val taskSize: SharedFile = {
      val tmp = java.io.File.createTempFile("size", ".txt")
      val writer = new java.io.FileWriter(tmp)
      writer.write("10000")
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

    // val piTask = piCalc(PiInput(None, None))(CPUMemoryRequest(1, 50))
    //
    // 1 to numTasks foreach { i =>
    //   val batch =
    //     batchCalc(BatchInput(Some(taskSize)))(CPUMemoryRequest(1, 50))
    //   batch ~> piTask
    // }

    println(Await.result(pi, atMost = 10 minutes))

  }

}
