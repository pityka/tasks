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
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global

import org.ekrich.config.ConfigFactory
import tasks.jsonitersupport._
import tasks.JvmElasticSupport.JvmGrid

import scala.concurrent.duration._

object WorkerHealthUrlFileTest extends TestHelpers {

  val testTask = Task[Input, Int]("workerhealthurlfiletest", 1) { _ => _ =>
    IO(1)
  }

  // A path the worker writes its messenger base URL to. Read back in the test
  // to assert the worker honoured tasks.worker.healthUrlFile.
  val healthFile: java.io.File = {
    val tmp = tasks.util.TempFile.createTempFile(".url")
    tmp.delete()
    tmp
  }

  val testConfig2 = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=0
      tasks.disableRemoting = false
      tasks.elastic.queueCheckInterval = 1 seconds
      tasks.elastic.idleNodeTimeout = 60 seconds
      tasks.askInterval = 250 millis
      tasks.addShutdownHook = false
      tasks.failuredetector.acceptable-heartbeat-pause = 10 s
      tasks.elastic.maxNodes = 1
      """
    )
  }

  val workerConfig =
    s"""
    tasks.elastic.idleNodeTimeout = 60 seconds
    tasks.askInterval = 250 millis
    tasks.worker.healthUrlFile = "${healthFile.getAbsolutePath}"
    """

  def run: IO[Option[String]] =
    JvmGrid.make(None, workerConfig).use { case (_, elasticSupport) =>
      val es: Resource[IO, Option[tasks.elastic.ElasticSupport]] =
        Resource.pure(Some(elasticSupport))
      val program = withTaskSystem(
        Some(testConfig2),
        Resource.pure(None),
        es,
        Resource.pure(None)
      ) { implicit ts =>
        // One submission → one worker spawn. Wait for the worker to come up and
        // write its URL file.
        testTask(Input(1))(ResourceRequest(1, 500)).flatMap { _ =>
          // Poll the URL file for up to ~5s in 200ms ticks.
          def waitForFile(remaining: Int): IO[Option[String]] =
            IO(healthFile.exists()).flatMap {
              case true =>
                IO(java.nio.file.Files.readString(healthFile.toPath))
                  .map(Some(_))
              case false if remaining <= 0 => IO.pure(None)
              case false =>
                IO.sleep(200.millis) *> waitForFile(remaining - 1)
            }
          waitForFile(25)
        }
      }
      program.flatMap {
        case Right(value) => IO.pure(value)
        case Left(exit)   => IO.raiseError(new RuntimeException(s"exit: $exit"))
      }
    }

}

class WorkerHealthUrlFileTestSuite extends FunSuite with Matchers {

  test(
    "worker writes its messenger base URL to tasks.worker.healthUrlFile"
  ) {
    val contents = WorkerHealthUrlFileTest.run.unsafeRunSync()
    contents should not be None
    val url = contents.get
    url should startWith("http://")
    // The file holds only the base URL (host:port), not the prefix-suffixed
    // messenger URL. So it must not end with the worker's bindPrefix path.
    url should endWith("/")
  }

}
