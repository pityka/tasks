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
import cats.effect.unsafe.implicits.global

import org.scalatest.matchers.should.Matchers

import tasks.jsonitersupport._
import org.ekrich.config.ConfigFactory
import cats.effect.IO
import cats.effect.kernel.Resource
import tasks.JvmElasticSupport.JvmGrid
import scala.concurrent.duration._

object IdleNodeShutdownTest extends TestHelpers {

  val testTask = Task[Input, Int]("idlenodeshutdowntest", 1) { _ => _ =>
    IO(1)
  }

  val testConfig2 = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=0
      tasks.disableRemoting = false
      tasks.elastic.queueCheckInterval = 1 seconds
      tasks.elastic.idleNodeTimeout = 3 seconds
      tasks.askInterval = 250 millis
      tasks.addShutdownHook = false
      tasks.failuredetector.acceptable-heartbeat-pause = 10 s
      """
    )
  }

  // Worker config must include the short idleNodeTimeout / askInterval too —
  // JvmCreateNode spawns workers via defaultTaskSystem with their own config,
  // they do not inherit the master's settings.
  val workerConfig =
    """
    tasks.elastic.idleNodeTimeout = 3 seconds
    tasks.askInterval = 250 millis
    """

  def run: IO[(Int, List[String])] =
    JvmGrid.make(None, workerConfig).use { case (control, elasticSupport) =>
      val es: Resource[IO, Option[tasks.elastic.ElasticSupport]] =
        Resource.pure(Some(elasticSupport))
      val program = withTaskSystem(
        Some(testConfig2),
        Resource.pure(None),
        es,
        Resource.pure(None)
      ) { implicit ts =>
        val ios =
          (1 to 3).toList.map(i => testTask(Input(i))(ResourceRequest(1, 500)))
        for {
          results <- ios.parSequence
          // Wait > idleNodeTimeout (3s) plus a couple of askInterval ticks
          // for the launcher to observe idleness and self-shutdown.
          _ <- IO.sleep(8.seconds)
          shutdowns <- control.shutdowns
        } yield (results.sum, shutdowns)
      }
      program.flatMap {
        case Right(value) => IO.pure(value)
        case Left(exit)   => IO.raiseError(new RuntimeException(s"exit: $exit"))
      }
    }

}

class IdleNodeShutdownTestSuite extends FunSuite with Matchers {

  test(
    "an idle worker spawned by elastic allocation should shut itself down after idleNodeTimeout"
  ) {
    val (sum, shutdowns) = IdleNodeShutdownTest.run.unsafeRunSync()
    sum should equal(3)
    shutdowns should not be empty
  }

}
