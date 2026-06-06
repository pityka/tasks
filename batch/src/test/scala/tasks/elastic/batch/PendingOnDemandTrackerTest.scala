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

package tasks.elastic.batch

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.parallel._

import java.util.concurrent.TimeUnit

class PendingOnDemandTrackerTest extends AnyFunSuite with Matchers {

  test(
    "concurrent tryReserve cannot collectively exceed cap when desired is stale"
  ) {
    // Simulate the burst case: many fibers enter selectJobQueue concurrently,
    // each seeing the same stale `desiredVcpus`. Without the tracker they'd
    // all accept and overshoot the cap. With it, accepts must sum to <= cap -
    // desired.
    val parallelism = 32
    val desired = 32
    val cap = 512
    val ask = 64
    // Budget under the stale desired: (512 - 32) = 480, so at most 7 accepts
    // of 64 vCPUs (= 448), since the 8th would push effective to 32+448+64=544.
    val expectedMaxAccepts = (cap - desired) / ask // == 7

    val program = for {
      tracker <- PendingOnDemandTracker.make(TimeUnit.SECONDS.toNanos(60))
      now <- IO.monotonic.map(_.toNanos)
      results <- (0 until parallelism).toList.parTraverse { _ =>
        tracker.tryReserve(now, ask, desired, cap)
      }
    } yield results

    val results = program.unsafeRunSync()
    val accepts = results.count(_._1)
    accepts shouldBe expectedMaxAccepts
  }

  test("entries older than the TTL are pruned on the next tryReserve") {
    val ttl = TimeUnit.MILLISECONDS.toNanos(50)
    val cap = 100
    val ask = 60

    val program = for {
      tracker <- PendingOnDemandTracker.make(ttl)
      now <- IO.monotonic.map(_.toNanos)
      first <- tracker.tryReserve(now, ask, desiredVcpus = 0, cap = cap)
      // A second reservation at the same time would push pending to 120 > cap.
      second <- tracker.tryReserve(now, ask, desiredVcpus = 0, cap = cap)
      // After the TTL elapses (we pass a synthetic later timestamp), the
      // first entry must be pruned.
      later = now + ttl + 1
      third <- tracker.tryReserve(later, ask, desiredVcpus = 0, cap = cap)
    } yield (first, second, third)

    val (first, second, third) = program.unsafeRunSync()
    first shouldBe ((true, 0))
    second._1 shouldBe false
    third shouldBe ((true, 0))
  }

  test("rejected reservations do not pollute the pending counter") {
    val cap = 100

    val program = for {
      tracker <- PendingOnDemandTracker.make(TimeUnit.SECONDS.toNanos(60))
      now <- IO.monotonic.map(_.toNanos)
      a <- tracker.tryReserve(now, addCpu = 80, desiredVcpus = 0, cap = cap)
      // Would overshoot — must be rejected and not added.
      b <- tracker.tryReserve(now, addCpu = 80, desiredVcpus = 0, cap = cap)
      // The next request with a fitting size must still see only 80 pending.
      c <- tracker.tryReserve(now, addCpu = 20, desiredVcpus = 0, cap = cap)
    } yield (a, b, c)

    val (a, b, c) = program.unsafeRunSync()
    a._1 shouldBe true
    b._1 shouldBe false
    c._1 shouldBe true
    c._2 shouldBe 80
  }
}
