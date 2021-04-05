/*
 * The MIT License
 *
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

package example

import tasks._
import tasks.circesupport._

import scala.concurrent._
import scala.concurrent.duration._
import io.circe.generic.auto._

/** Definitions of subtasks for calculating Pi
  *
  * We define two tasks:
  *   - `batchCalc` throws points to a square and count those within the unit circle
  *   - `piCalc` calculates Pi based on the number of points inside/outside
  *
  * Both of these need case classes to hold inputs and results.
  */
object PiTasks {
  case class BatchResult(inside: Int, outside: Int)

  case class BatchInput(batchSize: SharedFile, batchId: Int)

  case class PiInput(inside: Int, outside: Int)

  case class PiResult(pi: Double)

  /** Task definition
    *
    * Specifies input output types and name of task.
    * The tasks's body is an Input => tasks.ComputationEnvironment => Output function
    */
  val batchCalc = AsyncTask[BatchInput, BatchResult]("batch", 1) {

    /* Input of task, does not need to be a pattern match */
    case BatchInput(sizeFile: SharedFile, id: Int) =>
      implicit ctx =>
        /* SharedFile#file downloads the file to the local tmp folder */
        val localFile: Future[java.io.File] = sizeFile.file

        audit(s"Computing pi, part $id")

        localFile.map { localFile =>
          // Body of the task
          val sizeInt = scala.io.Source.fromFile(localFile).mkString.toInt
          val (in, out) = (0 until sizeInt).foldLeft((0, 0)) {
            case ((countIn, countOut), _) =>
              val x = scala.util.Random.nextDouble()
              val y = scala.util.Random.nextDouble()
              val inside = math.sqrt(x * x + y * y) <= 1.0
              if (inside) (countIn + 1, countOut) else (countIn, countOut + 1)
          }

          /* Return value */
          BatchResult(in, out)

        }
  }

  val piCalc = AsyncTask[PiInput, PiResult]("reduce", 1) {
    case PiInput(in, out) =>
      _ => Future.successful(PiResult(in.toDouble / (in + out) * 4d))
  }
}

/** Recursive algorithm of the n-th Fibonacci number
  *
  * Demonstrates how to recursively spawn new tasks from a task
  *
  * Defines two tasks:
  *   - `reduce` calculates (n-1)+(n-2)
  *   - `fibtask` spawns the necessary tasks
  */
object Fib {

  case class FibInput(n: Int)

  case class FibReduce(f1: Int, f2: Int)

  val reduce = AsyncTask[FibReduce, Int]("fibreduce", 1) {
    case FibReduce(f1, f2) =>
      _ => Future.successful(f1 + f2)
  }

  /** Recursive Fibonacci
    *
    * Spawns new subtasks, which return in a future,
    * thus this is an asynchronous task as well.
    *
    * The implicit context provides an ExecutionContext in which the Futures
    * and the body of the task is running.
    */
  val fibtask: TaskDefinition[FibInput, Int] =
    AsyncTask[FibInput, Int]("fib", 1) { case FibInput(n) =>
      implicit cxt =>
        n match {
          case 0 => Future.successful(0)
          case 1 => Future.successful(1)
          case n => {

            val f1 = fibtask(FibInput(n - 1))(ResourceRequest(1, 1, 1))
            val f2 = fibtask(FibInput(n - 2))(ResourceRequest(1, 1, 1))

            val f3: Future[Int] = for {
              r1 <- f1
              r2 <- f2
              r3 <- reduce(FibReduce(r1, r2))(ResourceRequest(1, 1, 1))

            } yield r3

            releaseResources

            f3

          }

        }

    }

}

object PiApp extends App {

  import PiTasks._
  import Fib._

  /** Opens and closes a TaskSystem with default configuration
    * On a slave node, the block is not executed,
    * but it starts pulling jobs from the queue
    */
  withTaskSystem { implicit ts =>
    val numTasks = 100

    import scala.concurrent.ExecutionContext.Implicits.global

    val taskSize: Future[SharedFile] = {
      val tmp = java.io.File.createTempFile("size", ".txt")
      val writer = new java.io.FileWriter(tmp)
      writer.write("1000")
      writer.close

      SharedFile(tmp, name = "taskSize.txt")
    }

    /* Start tasks for Pi */
    val pi: Future[PiResult] = taskSize.flatMap { taskSize =>
      Future
        .sequence(
          1 to numTasks map { i =>
            batchCalc(BatchInput(taskSize, i))(ResourceRequest(1, 1000, 1))
          }
        )
        .flatMap { batches =>
          piCalc(
            PiInput(batches.map(_.inside).sum, batches.map(_.outside).sum)
          )(ResourceRequest(1, 1000, 1))
        }
    }

    /* Start tasks for Fibonacci, subtasks are started by this task. */
    val fibResult = fibtask(FibInput(4))(ResourceRequest(1, 1000, 1))

    /* Block and wait for the futures */
    println(Await.result(pi, atMost = 10 minutes))
    println(Await.result(fibResult, atMost = 10 minutes))

  }

}
