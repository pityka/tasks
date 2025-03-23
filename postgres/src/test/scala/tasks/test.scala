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
import skunk.implicits._
import skunk.codec.all._
import skunk.data.TransactionIsolationLevel
import skunk.data.TransactionAccessMode
class PostgresSuite extends FunSuite with Matchers {
  import natchez.Trace.Implicits.noop
  test("skunk transaction") {
    skunk.Session
      .single[IO](
        host = "localhost",
        port = 5432,
        user = "postgres",
        database = "postgres",
        password = Some("mysecretpassword")
      )
      .use { session1 =>
        skunk.Session
          .single[IO](
            host = "localhost",
            port = 5432,
            user = "postgres",
            database = "postgres",
            password = Some("mysecretpassword")
          )
          .use { session2 =>
            Ref.of[IO, Boolean](false).flatMap { ref =>
              val prep = session1.execute(
                sql"create table if not exists testtx (value text)".command
              ) *>
                session1.execute(sql"insert into testtx values ('a')".command)
              val tx =
                (prep *> (session1
                  .transaction(
                    TransactionIsolationLevel.Serializable,
                    TransactionAccessMode.ReadWrite
                  )
                  .use { tx1 =>
                    session2
                      .transaction(
                        TransactionIsolationLevel.Serializable,
                        TransactionAccessMode.ReadWrite
                      )
                      .use { _ =>
                        (session1
                          .execute(sql"select * from testtx".query(text)) *>
                          session2
                            .execute(sql"select * from testtx".query(text)) *>
                          session1
                            .execute(
                              sql"update testtx set value = 'b'".command
                            ) *>
                          tx1.commit *>
                          session2
                            .execute(
                              sql"update testtx set value = 'c'".command
                            )).map(b => ref.set(true))
                      }
                  }))

              IO.uncancelable { poll =>
                poll(tx).flatten
              }.attempt
                .flatMap(_ => ref.get.map(result => assert(!result)))
            }
          }
      }
      .unsafeRunSync()
  }

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
        Ref.of[IO, Boolean](false).flatMap { ref =>
          tx.get *>
            tx.flatModify(old => State.empty -> IO.unit) *>
            tx.flatModify(old =>
              old.update(
                QueueImpl.LauncherJoined(LauncherActor(Address("sdfa")))
              ) -> ref.set(true)
            ) *> tx.get.map(state =>
              assert(
                state == State.empty.update(
                  QueueImpl.LauncherJoined(LauncherActor(Address("sdfa")))
                )
              )
            ) *> ref.get.map(a => assert(a))
        }
      }
      .unsafeRunSync()
  }
  test("postgres concurrent flatmodify") {
    tasks.queue.Postgres
      .makeTransaction(
        table = "table1",
        host = "localhost",
        port = 5432,
        user = "postgres",
        database = "postgres",
        password = Some("mysecretpassword")
      )
      .use { tx1 =>
        tasks.queue.Postgres
          .makeTransaction(
            table = "table1",
            host = "localhost",
            port = 5432,
            user = "postgres",
            database = "postgres",
            password = Some("mysecretpassword")
          )
          .use { tx2 =>
            Ref.of[IO, Int](0).flatMap { ref =>
              val io1 = tx1.flatModify(old =>
                old.update(
                  QueueImpl.LauncherJoined(LauncherActor(Address("AAA")))
                ) -> ref.update(_ + 1)
              )
              val io2 = tx2.flatModify(old =>
                old.update(
                  QueueImpl.LauncherJoined(LauncherActor(Address("BBB")))
                ) -> ref.update(_ + 1)
              )
              io1.both(io2) *> ref.get.map(a => assert(a == 2))
            }
          }
      }
      .unsafeRunSync()
  }

}
