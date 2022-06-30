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

package tasks.util

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import tasks.util.concurrent.NodeLocalCache

import cats.effect._
import cats.effect.implicits._
import cats.instances.all._
import cats.effect.unsafe.implicits.global
import scala.util.Random
import scala.concurrent.duration._

class CatsNodeLocalCacheTest extends FunSuite {

  test("cats-effect based node local cache ") {
    val program: IO[(List[(Int, Int)], Set[Int])] = for {
      garbage <- Ref.of[IO, Set[Int]](Set.empty)
      nlc <- NodeLocalCache.make[Int]
      s <- Random
        .shuffle((0 until 100).toList)
        .parTraverseN(100)(i =>
          IO {
            NodeLocalCache
              .offer(
                "id" + i / 10,
                Resource.make(IO {
                  println(s"allocating $i")
                  i
                }.delayBy(Random.nextInt(5000).millis))(i =>
                  IO.println(s"dealloc $i") *> garbage.update(_ + i)
                ),
                nlc
              )
              .use(i =>
                IO { println(s"using $i"); i }
                  .delayBy(Random.nextInt(5000).millis)
              )
          }.delayBy(Random.nextInt(5000).millis).flatten.map(r => (i, r))
        )
      _ <- IO.println("==================")
      s2 <- Random
        .shuffle((0 until 100).toList)
        .parTraverseN(100)(i =>
          IO {
            NodeLocalCache
              .offer(
                "id" + i / 10,
                Resource.make(IO {
                  println(s"allocating $i")
                  i
                }.delayBy(Random.nextInt(5000).millis))(i =>
                  IO.println(s"dealloc $i") *> garbage.update(_ + i)
                ),
                nlc
              )
              .use(i =>
                IO { println(s"using $i"); i }
                  .delayBy(Random.nextInt(5000).millis)
              )
          }.delayBy(Random.nextInt(5000).millis).flatten.map(r => (i, r))
        )
      g <- garbage.get
    } yield (s ++ s2, g)

    val (result, garbage) = program.unsafeRunSync()
    val sorted = result.sortBy(_._1).map(_._2)
    assert(sorted.size == 200)
    sorted.grouped(20).toList.zipWithIndex.foreach { case (group, idx) =>
      assert(group.distinct.size > 0)
      assert(group.distinct.size <= 10)
      assert(group.forall(g => garbage.contains(g)))
      assert(group.forall(g => g >= (idx * 10) && g < (idx + 1) * 10))
    }
  }

}
