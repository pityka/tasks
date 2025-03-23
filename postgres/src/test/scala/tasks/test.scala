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
import cats.effect.unsafe.implicits.global

import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import tasks.queue.QueueImpl.State
import tasks.queue.QueueImpl
import tasks.queue.Launcher.LauncherActor
import tasks.util.message.Address
import cats.effect.kernel.Ref

class PostgresSuite extends FunSuite with Matchers {

  test("postgres connects") {
    tasks.queue.Postgres
      .makeTransaction(
        table = "table1",
        host = "localhost",
        port = 5432,
        user = "postgres",
        database = "postgres",
        password = Some("mysecretpassword")
      )
      .use { tx =>
        Ref.of[IO,Boolean](false).flatMap{ ref =>
        tx.get *>
          tx.flatModify(old =>
            old.update(
              QueueImpl.LauncherJoined(LauncherActor(Address("sdfa")))
            ) -> ref.set(true)) *> tx.get.map(state =>
            assert(state == State.empty.update(
              QueueImpl.LauncherJoined(LauncherActor(Address("sdfa")))
            ))
          ) *> ref.get.map(a => assert(a))
        }
      }
      .unsafeRunSync()
  }

}
