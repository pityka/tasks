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
import org.ekrich.config.{Config, ConfigFactory}

import tasks.jsonitersupport._

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global

import tasks.deploy._
import tasks.elastic.ElasticSupport
import tasks.elastic.process.LocalShellElasticSupport
import tasks.util.config.TasksConfig

object AppOnlyMasterTest {

  case class In(n: Int)
  object In { implicit val codec: JsonValueCodec[In] = JsonCodecMaker.make }

  case class Out(n: Int)
  object Out { implicit val codec: JsonValueCodec[Out] = JsonCodecMaker.make }

  val doubler: ParentTaskDefinition[In, Out] =
    ParentTask[In, Out]("app-only-doubler", 1) { case In(n) =>
      _ => IO.pure(Out(n * 2))
    }
}

class AppOnlyMasterTestSuite
    extends FunSuite
    with Matchers
    with BeforeAndAfterAll {

  private val tmp = tasks.util.TempFile.createTempFile(".temp")
  tmp.delete()

  private val testConfigString: String = s"""
tasks.cache.enabled = false
tasks.disableRemoting = false
hosts.numCPU=0
hosts.gpus = []
tasks.askInterval = 20 ms
tasks.fileservice.storageURI=${tmp.getAbsolutePath}
tasks.worker-main-class = "tasks.TestWorker"
tasks.elastic.logQueueStatus = false
tasks.sh.contexts = [
  {
    context = c1
    hostname = localhost
    cpu = 1
    memory = 1000
  }
]
"""

  private val rawConfig: Config = ConfigFactory.parseString(testConfigString)

  private val appAndQueueOnlyHostConfig: HostConfiguration = {
    val tconfig: TasksConfig =
      tasks.util.config.parse(() => tasks.util.loadConfig(Some(rawConfig)))
    new DefaultHostConfigurationFromConfig()(tconfig) {
      override lazy val myRoles: Set[Role] = Set(App, Queue)
    }
  }

  private def withAppOnlyHostConfig(base: ElasticSupport): ElasticSupport =
    new ElasticSupport(
      hostConfig = Some(appAndQueueOnlyHostConfig),
      shutdownFromNodeRegistry = base.shutdownFromNodeRegistry,
      shutdownFromWorker = base.shutdownFromWorker,
      createNodeFactory = base.createNodeFactory,
      getNodeName = base.getNodeName
    )

  private val elasticResource: Resource[IO, Option[ElasticSupport]] =
    Resource
      .eval(LocalShellElasticSupport.make(Some(rawConfig)))
      .map(base => Some(withAppOnlyHostConfig(base)))

  private val pair = defaultTaskSystem(
    testConfigString,
    Resource.pure(None),
    elasticResource
  ).allocated.unsafeRunSync()

  implicit val system: TaskSystemComponents = pair._1._1

  import AppOnlyMasterTest._

  test(
    "0-resource ParentTask on an App+Queue master with no Worker role triggers an elastic spawn and returns its result"
  ) {
    val r = doubler(In(21)).unsafeRunSync()
    r.n shouldBe 42
  }

  override def afterAll() = {
    pair._2.unsafeRunSync()
  }
}
