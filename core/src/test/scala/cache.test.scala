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
import com.typesafe.config.ConfigFactory

import tasks.queue._
import tasks.caching._
import tasks.fileservice._
import tasks.util._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

import upickle.default._

case class A(x: Int)

class TaskCacheTestSuite extends FunSuite with BeforeAndAfterAll {

  val system = akka.actor
    .ActorSystem("cachetest", ConfigFactory.load("akkaoverrides.conf"))

  implicit val fsp = FileServicePrefix(Vector("/"))

  test("simple") {
    val file = TempFile.createTempFile(".mapdb")
    file.delete

    val cache =
      LevelDBCache(file, akka.serialization.SerializationExtension(system))
    val td = TaskDescription(
        TaskId(tasks.simpletask.SimpleTask.runTask.getClass.getName, 1),
        JsonString(
            write(tasks.simpletask.SimpleTask.MyResultSet(Some(1), Some(0)))),
        None
    )
    Await.result(
        cache.set(
            td,
            UntypedResult(Set(),
                          JsonString(write(tasks.simpletask.IntResult(1))))
        ),
        30 seconds)
    cache.shutDown

    val cache2 =
      LevelDBCache(file, akka.serialization.SerializationExtension(system))

    val read =
      Await.result(cache2
                     .get(td)
                     .map(x =>
                           upickle.default.read[tasks.simpletask.IntResult](
                               x.get.data.value)),
                   40 seconds)

    expectResult((tasks.simpletask.IntResult(1)))(read)

  }

  test("1000 elements") {

    val file2 = TempFile.createTempFile(".mapdb")
    file2.delete

    println(file2)

    val cache =
      LevelDBCache(file2, akka.serialization.SerializationExtension(system))
    for (i <- 1 to 1000) {
      val td = TaskDescription(
          TaskId(tasks.simpletask.SimpleTask.runTask.getClass.getName, 1),
          JsonString(
              write(
                  tasks.simpletask.SimpleTask.MyResultSet(Some(i), Some(0)))),
          None
      )
      Await.result(
          cache.set(
              td,
              UntypedResult(Set(),
                            JsonString(write(tasks.simpletask.IntResult(i))))
          ),
          30 seconds)
    }
    cache.shutDown

    val cache2 =
      LevelDBCache(file2, akka.serialization.SerializationExtension(system))

    for (i <- 1 to 1000) {
      val td = TaskDescription(
          TaskId(tasks.simpletask.SimpleTask.runTask.getClass.getName, 1),
          JsonString(
              write(
                  tasks.simpletask.SimpleTask.MyResultSet(Some(i), Some(0)))),
          None
      )
      val read =
        Await.result(cache2
                       .get(td)
                       .map(x =>
                             upickle.default.read[tasks.simpletask.IntResult](
                                 x.get.data.value)),
                     40 seconds)
      expectResult(((tasks.simpletask.IntResult(i))))(read)
    }

  }

}
