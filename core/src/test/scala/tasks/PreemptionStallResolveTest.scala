/*
 * The MIT License
 *
 * Copyright (c) 2026 Istvan Bartha
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

/** Stall-and-resolve: a recursive task on a 1-CPU launcher would deadlock
  * without preemption because the parent holds the only slot while it
  * waits for children. With preemption, the queue detects the stall
  * (every running task has descendants queued) and cancels the parent
  * to free its slot for a child. The cancelled parent is re-enqueued
  * and runs later when its dependencies are ready.
  */
object PreemptionStallResolveTest {

  case class FibIn(n: Int)
  object FibIn {
    implicit val codec: JsonValueCodec[FibIn] = JsonCodecMaker.make
  }

  case class FibOut(n: Int)
  object FibOut {
    implicit val codec: JsonValueCodec[FibOut] = JsonCodecMaker.make
  }

  val fibtask: TaskDefinition[FibIn, FibOut] =
    Task[FibIn, FibOut]("fib-preempt", 1) { case FibIn(n) =>
      { implicit ce =>
        n match {
          case 0 => IO.pure(FibOut(0))
          case 1 => IO.pure(FibOut(1))
          case n =>
            val f1 = fibtask(FibIn(n - 1))(ResourceRequest(1, 1))
            val f2 = fibtask(FibIn(n - 2))(ResourceRequest(1, 1))
            IO.both(f1, f2).map { case (a, b) => FibOut(a.n + b.n) }
        }
      }
    }
}

class PreemptionStallResolveTestSuite
    extends FunSuite
    with Matchers
    with BeforeAndAfterAll
    with TestHelpers {

  override val testConfig = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""
      tasks.cache.enabled = true
      tasks.disableRemoting = true
      hosts.numCPU = 1
      tasks.askInterval = 200 millis
      tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      """
    )
  }

  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .withHandler(minimumLevel = Some(scribe.Level.Info))
    .replace()

  val pair = defaultTaskSystem(Some(testConfig)).allocated.unsafeRunSync()
  implicit val system: TaskSystemComponents = pair._1._1
  import PreemptionStallResolveTest._

  // Without preemption, this would deadlock: fib(n) holds the only CPU
  // slot while awaiting fib(n-1) and fib(n-2), which can never be
  // scheduled. With preemption, the queue cancels the waiting parent,
  // runs the children, then re-runs the parent which hits the cache.
  test("preemption resolves recursive-task stall on a single-CPU launcher") {
    val n = 5
    val expected = {
      def serial(i: Int): Int =
        if (i <= 1) i else serial(i - 1) + serial(i - 2)
      serial(n)
    }
    val r = fibtask(FibIn(n))(ResourceRequest(1, 1))
      .unsafeRunTimed(scala.concurrent.duration.DurationInt(30).seconds)
      .getOrElse(throw new RuntimeException("timeout"))
    assertResult(expected)(r.n)
  }

  override def afterAll() = {
    pair._2.unsafeRunSync()
  }
}
