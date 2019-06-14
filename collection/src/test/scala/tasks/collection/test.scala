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

package tasks.ecoll

import org.scalatest._

import tasks._
import tasks.circesupport._

import scala.concurrent._
import scala.concurrent.duration._
import io.circe.generic.auto._

object Tests {

  implicit val s1l = makeSerDe[Long]
  implicit val s1l2 = makeSerDe[(Option[Option[Int]], Option[Int])]
  implicit val s1i = spore(() => implicitly[tasks.queue.Serializer[Int]])
  implicit val s2i = spore(() => implicitly[tasks.queue.Deserializer[Int]])
  implicit val s1 = spore(() => implicitly[tasks.queue.Serializer[Option[Int]]])
  implicit val s2 = spore(
    () => implicitly[tasks.queue.Deserializer[Option[Int]]])
  implicit val s3 = spore(() => implicitly[tasks.queue.Deserializer[String]])
  implicit val s4 = spore(() => implicitly[tasks.queue.Serializer[String]])
  implicit val s5 = spore(
    () => implicitly[tasks.queue.Deserializer[Seq[Option[Int]]]])
  implicit val s6 = spore(
    () => implicitly[tasks.queue.Serializer[Seq[Option[Int]]]])
  implicit val s7 = spore(
    () => implicitly[tasks.queue.Deserializer[Seq[Option[Option[Int]]]]])
  implicit val s8 = spore(
    () => implicitly[tasks.queue.Serializer[Seq[Option[Option[Int]]]]])
  implicit val s9 = spore(
    () =>
      implicitly[tasks.queue.Deserializer[(Option[Option[Int]],
                                           Option[Option[Int]])]])
  implicit val s10 = spore(
    () =>
      implicitly[tasks.queue.Serializer[(Option[Option[Int]],
                                         Option[Option[Int]])]])

  val twice =
    EColl.map("twice", 1)((_: Option[Int]).map(_ * 3))

  val collect = EColl.collect[Option[Int], String]("doubleodd", 1)(
    (i: Option[Int]) =>
      if (i.isDefined) Some(i.get.toString)
      else None)

  val odd =
    EColl.filter("odd", 1)(spore((i: Option[Int]) => i.forall(_ % 2 == 1)))

  val scan =
    EColl.scan("sum", 1)(spore[(Option[Int], Option[Int]), Option[Int]] {
      case (sum, elem) => Some(sum.get + elem.get)
    })

  val mapconcat =
    EColl.mapConcat("mapconcat", 1)(
      spore[Option[Int], Seq[Int]]((a: Option[Int]) => (1 to a.get).toIterable))

  val sort =
    EColl.sortBy("sortByToString", 1)((_: Option[Int]).toString)

  val group =
    EColl.groupBy("groupByToConstant", 1)(spore(() => Option(4)),
                                          spore(() => None),
                                          (_: Option[Int]) => "a")
  val groupPresorted =
    EColl.groupByPresorted("groupByPresorted", 1)((_: Option[Int]) => "a")

  val join =
    EColl.join("outerjoinByToString", 1, Some(3), None)(
      (_: Option[Int]).toString)

  val join2 =
    EColl.join2LeftOuter("leftouter2joinByToString", 1, Some(3), None)(
      (_: Option[Int]).toString,
      (_: Option[Int]).toString)

  val take = EColl.take[Option[Int]]("take1", 1)

  val count =
    EColl.fold[Option[Int], Option[Int]]("count", 1)(spore {
      case (x: Option[Int], _: Option[Int]) =>
        Option(x.get + 1)
    })

  val toSeq = EColl.grouped[Option[Int]]("toseq", 1)
  val distinct = EColl.distinct[Option[Int]]("distinct", 1)

  val sum2 =
    EColl.reduce("sum2", 1)(spore[(Option[Int], Option[Int]), Option[Int]] {
      case (x, y) => Some(x.get + y.get)
    })

  def run(folder: String) = {

    withTaskSystem(
      s"tasks.fileservice.storageURI=$folder"
    ) { implicit ts =>
      import scala.concurrent.ExecutionContext.Implicits.global

      val mappedEColl = for {
        scanZero <- EColl.single(Option(0))
        e1 <- EColl.fromSource(
          akka.stream.scaladsl.Source(List(3, 2, 1).map(Option(_))))
        s1 <- collect(e1)(ResourceRequest(1, 1))
        e2 <- twice(e1)(ResourceRequest(1, 1))
        sum <- scan((e1, scanZero))(ResourceRequest(1, 1))
        v2 <- e2.toSeq(1)
        sv2 <- s1.toSeq(1)
        e3 <- odd(e2)(ResourceRequest(1, 1))
        sumSeqV <- sum.toSeq(1)
        mapconcat <- mapconcat(e1)(ResourceRequest(1, 1))
        mapconcatV <- mapconcat.toSeq(1)
        e4 <- sort(e3)(ResourceRequest(1, 1))
        e4V <- e4.toSeq(1)
        grouped <- group(e4)(ResourceRequest(1, 1))
        gV <- grouped.toSeq(1)
        grouped2 <- groupPresorted(e4)(ResourceRequest(1, 1))
        gV2 <- grouped2.toSeq(1)
        e6 <- join(List(e1 -> false, e2 -> true, e3 -> false))(
          ResourceRequest(1, 1))
        e6V <- e6.toSeq(1)
        e7 <- join2((e1, e2))(ResourceRequest(1, 1))
        e7V <- e7.toSeq(1)
        e8 <- take(e1 -> 1L)(ResourceRequest(1, 1))
        e8V <- e8.toSeq(1)
        e9 <- count(e1 -> Some(0))(ResourceRequest(1, 1))
        e9V <- e9.toSeq(1)
        e10 <- sum2(e1)(ResourceRequest(1, 1)).flatMap(_.toSeq(1))
        distinct <- distinct(e4)(ResourceRequest(1, 1)).flatMap(_.toSeq(1))

      } yield
        (v2,
         sv2,
         sumSeqV,
         mapconcatV,
         e4V,
         gV,
         gV2,
         e6V,
         e7V,
         e8V,
         e9V.head,
         e10.head,
         distinct)

      Await.result(mappedEColl, atMost = 10 minutes)
    }

  }

}

class TaskCollectionTestSuite extends FunSuite with Matchers {

  val expected =
    (Vector(Some(9), Some(6), Some(3)),
     Vector(3, 2, 1),
     Vector(Some(0), Some(3), Some(5), Some(6)),
     Vector(1, 2, 3, 1, 2, 1),
     Vector(Some(3), Some(9)),
     Vector(List(Some(3), Some(9))),
     Vector(List(Some(3), Some(9))),
     Vector(List(Some(Some(3)), Some(Some(3)), Some(Some(3))),
            List(None, Some(Some(6)), None),
            List(None, Some(Some(9)), Some(Some(9)))),
     Vector((Some(Some(3)), Some(3)), (None, Some(6)), (None, Some(9))),
     Vector(Some(3)),
     Some(3),
     Some(6),
     Vector(Some(3), Some(9)))

  val got =
    (Vector(Some(9), Some(6), Some(3)),
     Vector(3, 2, 1),
     Vector(Some(0), Some(3), Some(5), Some(6)),
     Vector(1, 2, 3, 1, 2, 1),
     Vector(Some(3), Some(9)),
     Vector(List(Some(3), Some(9))),
     Vector(List(Some(3), Some(9))),
     Vector(List(Some(Some(3)), Some(Some(3)), Some(Some(3))),
            List(None, Some(Some(6)), None),
            List(None, Some(Some(9)), Some(Some(9)))),
     Vector((Some(Some(3)), Some(3)), (None, Some(6)), (None, Some(9))),
     Vector(Some(3)),
     Some(3),
     Some(6),
     Vector(Some(3), Some(9)))

  test("collection") {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    Tests.run(tmp.getAbsolutePath).get.toString shouldBe expected.toString
  }

}
