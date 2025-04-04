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

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}

import org.scalatest.matchers.should.Matchers
import tasks.queue.Spore

import tasks.util._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

import org.ekrich.config.ConfigFactory
import cats.effect.IO

object SporeTest extends TestHelpers with Matchers {

  val increment =
    Task[Spore[Option[Int], String], String]("sporetest", 1) { sp => _ =>
      IO(sp(Some(3)))
    }

  def run = {
    val tmp = TempFile.createTempFile(".temp")
    tmp.delete
    withTaskSystem(
      Some(
        ConfigFactory.parseString(
          s"tasks.fileservice.storageURI=${tmp.getAbsolutePath}\n"
        )
      )
    ) { implicit ts =>
      ((increment(spore { (a: Option[Int]) =>
        a.toString
      })(ResourceRequest(1, 500))))

    }
  }

}

object MySpore {
  val s = spore { (a: Option[Int]) =>
    a.toString
  }

  case class User(a: String)
  object User {
    // without this the compiler blows up
    implicit val codec: JsonValueCodec[User] = JsonCodecMaker.make

    implicit val encoderSpore: Spore[Unit, tasks.queue.Serializer[User]] =
      spore((_: Unit) => implicitly[tasks.queue.Serializer[User]])
  }
  val serializerSpore = spore { (_: String) =>
    implicitly[tasks.queue.Serializer[User]]
  }

  val nullary = spore { (_: Unit) =>
    implicitly[tasks.queue.Serializer[User]]
  }

  def mkSpore[T] = spore { (a: List[T]) =>
    val f = (a: Int) => a.toString
    val _ = f(1)
    a.toString
  }

  val g1 = mkSpore[String]
  val g2 = mkSpore[Int]

  def freeze[A, B](s: Spore[A, B]) = s.copy[A, B]()

  def useSpore(s: Spore[String, Option[Int]]) = {
    val s1 = "3"
    freeze(s)(s1)
  }
  def useSpore0(s: Spore[Unit, Option[Int]]) = {
    freeze(s)(())
  }

  val sporeLeaf = spore { (a: String) =>
    a.toUpperCase()
  }

  def recursive(s: Spore[String, String]) = spore { (a: String) =>
    s(a)
  }

  val prim = spore { (_: Long) =>
    3
  }

  val prim2 = spore { (_: Unit) =>
    3
  }

  // must not compile
  // class Outer {
  //   val spo = spore((a: String) => None)
  // }

}

class SporeTestSuite extends FunSuite with Matchers {

  def freeze[A, B](s: Spore[A, B]) = s.copy[A, B]()

  test("spores should revive") {
    freeze(MySpore.s)(Some(3)) shouldBe "Some(3)"
  }
  test("generic spores should revive") {
    freeze(MySpore.g1)(List("1")) shouldBe "List(1)"
    freeze(MySpore.g2)(List(1)) shouldBe "List(1)"
  }

  test("static serializer should revive from nullary spore") {
    new String(
      freeze(MySpore.nullary)(()).apply(MySpore.User("1"))
    ) shouldBe """{"a":"1"}"""
  }
  test("static serializer should revive from nullary implicit spore") {
    val sp =
      freeze(implicitly[Spore[Unit, tasks.queue.Serializer[MySpore.User]]])
    new String(sp.apply(()).apply(MySpore.User("1"))) shouldBe """{"a":"1"}"""
  }

  test("static serializer should revive") {
    new String(
      freeze(MySpore.serializerSpore)("").apply(MySpore.User("1"))
    ) shouldBe """{"a":"1"}"""
  }

  test("spores should work in tasks") {
    import cats.effect.unsafe.implicits.global

    SporeTest.run.unsafeRunSync().toOption.get should equal("Some(3)")
  }
  test("primitive return type") {
    MySpore.prim(1L) shouldBe 3
    MySpore.prim2(()) shouldBe 3
  }

  test("recursive") {
    val spore = MySpore.recursive(MySpore.sporeLeaf)
    spore("abcd") shouldBe "ABCD"

    readFromString[Spore[String, String]](writeToString(spore)).apply(
      "qwerty"
    ) shouldBe "QWERTY"
  }

}
