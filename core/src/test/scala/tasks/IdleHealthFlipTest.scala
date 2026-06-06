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
import org.http4s.{Method, Request, Status, Uri}
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

import tasks.jsonitersupport._
import tasks.JvmElasticSupport.JvmGrid

import scala.concurrent.duration._

object IdleHealthFlipTest extends TestHelpers {

  val testTask = Task[Input, Int]("idlehealthfliptest", 1) { _ => _ => IO(1) }

  val healthFile: java.io.File = {
    val tmp = tasks.util.TempFile.createTempFile(".url")
    tmp.delete()
    tmp
  }

  val testConfig2 = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete()
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=0
      tasks.disableRemoting = false
      tasks.elastic.queueCheckInterval = 1 seconds
      tasks.elastic.idleNodeTimeout = 2 seconds
      tasks.askInterval = 250 millis
      tasks.addShutdownHook = false
      tasks.failuredetector.acceptable-heartbeat-pause = 10 s
      tasks.elastic.maxNodes = 1
      """
    )
  }

  val workerConfig =
    s"""
    tasks.elastic.idleNodeTimeout = 2 seconds
    tasks.askInterval = 250 millis
    tasks.worker.healthUrlFile = "${healthFile.getAbsolutePath}"
    """

  private def client: Resource[IO, Client[IO]] =
    EmberClientBuilder.default[IO].build

  private def healthStatus(c: Client[IO], baseUrl: String): IO[Status] =
    c.run(
      Request[IO](
        method = Method.GET,
        uri = Uri.fromString(baseUrl + "health").toOption.get
      )
    ).use(resp => IO.pure(resp.status))
      // After teardown the server stops accepting connections — treat any IO
      // failure as a not-OK status so polling can terminate.
      .handleError(_ => Status.InternalServerError)

  def run: IO[(Status, List[Status])] =
    JvmGrid.make(None, workerConfig).use { case (_, elasticSupport) =>
      val es: Resource[IO, Option[tasks.elastic.ElasticSupport]] =
        Resource.pure(Some(elasticSupport))
      val program = withTaskSystem(
        Some(testConfig2),
        Resource.pure(None),
        es,
        Resource.pure(None)
      ) { implicit ts =>
        client.use { c =>
          // Drive the worker spawn.
          testTask(Input(1))(ResourceRequest(1, 500)) *> {
            // Wait for the worker to advertise its base URL.
            def waitForUrl(remaining: Int): IO[String] =
              IO(healthFile.exists()).flatMap {
                case true =>
                  IO(java.nio.file.Files.readString(healthFile.toPath))
                case false if remaining <= 0 =>
                  IO.raiseError(
                    new RuntimeException("worker never wrote health URL file")
                  )
                case false =>
                  IO.sleep(100.millis) *> waitForUrl(remaining - 1)
              }
            waitForUrl(50).flatMap { url =>
              for {
                healthy <- healthStatus(c, url)
                // Poll for the flip: the launcher fires self-shutdown a few
                // ticks after task completion + idleNodeTimeout (2s). Watch
                // until we observe a non-OK status, capped at ~12s.
                samples <- {
                  def loop(
                      acc: List[Status],
                      remaining: Int
                  ): IO[List[Status]] =
                    if (remaining <= 0) IO.pure(acc.reverse)
                    else
                      healthStatus(c, url).flatMap { s =>
                        val acc1 = s :: acc
                        if (s != Status.Ok) IO.pure(acc1.reverse)
                        else IO.sleep(50.millis) *> loop(acc1, remaining - 1)
                      }
                  loop(Nil, 240)
                }
              } yield (healthy, samples)
            }
          }
        }
      }
      program.flatMap {
        case Right(v)   => IO.pure(v)
        case Left(exit) => IO.raiseError(new RuntimeException(s"exit: $exit"))
      }
    }
}

class IdleHealthFlipTestSuite extends FunSuite with Matchers {

  test(
    "/health flips off Ok once the launcher initiates idle self-shutdown"
  ) {
    val (firstHealthy, samples) =
      IdleHealthFlipTest.run.unsafeRunSync()
    firstHealthy shouldBe Status.Ok
    // Final sample must not be Ok — either 503 (shutdownInitiated flipped) or
    // a synthetic InternalServerError standing in for connection failure
    // after the worker has torn down its HTTP server.
    samples.last should not be Status.Ok
  }

}
