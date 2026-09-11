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
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import org.ekrich.config.{Config, ConfigFactory}
import scala.concurrent.duration._

import tasks.queue.QueueImpl
import tasks.util.Transaction

object MainProcessResourceTest {

  private def storageFolder = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    tmp.mkdirs
    tmp.getAbsolutePath
  }

  def appConfig: Config = ConfigFactory.parseString(
    s"""tasks.fileservice.storageURI=$storageFolder
      tasks.cache.enabled = false
      hosts.numCPU = 1
      tasks.askInterval = 20 ms
      tasks.addShutdownHook = false
      """
  )

  def workerConfig: Config = ConfigFactory.parseString(
    s"""tasks.fileservice.storageURI=$storageFolder
      tasks.cache.enabled = false
      hosts.numCPU = 1
      hosts.app = false
      hosts.master = "localhost:1"
      tasks.askInterval = 20 ms
      tasks.disableRemoting = false
      tasks.addShutdownHook = false
      """
  )

  def recorder(events: Ref[IO, List[String]]): Resource[IO, Unit] =
    Resource.make(events.update(_ :+ "acquire"))(_ =>
      events.update(_ :+ "release")
    )

}

class MainProcessResourceTestSuite extends FunSuite with Matchers {

  import MainProcessResourceTest._

  test(
    "the main process resource is open while the body of an app process runs and is closed after it"
  ) {
    val program = Ref.of[IO, List[String]](Nil).flatMap { events =>
      withTaskSystem(appConfig, recorder(events))(_ => events.get)
        .flatMap(duringBody =>
          events.get.map(afterBody => (duringBody, afterBody))
        )
    }

    val (duringBody, afterBody) = program.unsafeRunSync()

    duringBody shouldBe Right(List("acquire"))
    afterBody shouldBe List("acquire", "release")
  }

  test("a worker process does not acquire the main process resource") {
    val program = for {
      events <- Ref.of[IO, List[String]](Nil)
      queueState <- Ref.of[IO, QueueImpl.State](QueueImpl.State.empty)
      worker = withTaskSystem(
        config = Some(workerConfig),
        s3Client = Resource.pure(None),
        elasticSupport = Resource.pure(None),
        externalQueueState =
          Resource.pure(Some(Transaction.fromRef(queueState))),
        meterProvider = Resource.pure[
          IO,
          org.typelevel.otel4s.metrics.MeterProvider[IO]
        ](org.typelevel.otel4s.metrics.MeterProvider.noop[IO]),
        mainProcessResource = recorder(events)
      )(_ => IO.unit)
      fiber <- worker.start
      _ <- (IO.sleep(50.millis) *> queueState.get.map(
        _.knownLaunchers.nonEmpty
      )).iterateUntil(identity).timeout(60.seconds)
      recorded <- events.get
      _ <- fiber.cancel
    } yield recorded

    program.unsafeRunSync() shouldBe Nil
  }

}
