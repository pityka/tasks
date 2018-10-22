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

import akka.actor.ActorSystem
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent._

import tasks.wire._
import tasks.queue._
import tasks.fileservice._
import tasks.util.config.TasksConfig
import tasks.deploy._

import scala.language.experimental.macros

package object tasks {

  val SharedFile = tasks.fileservice.SharedFile

  type SharedFile = tasks.fileservice.SharedFile

  type CodeVersion = tasks.shared.CodeVersion
  val CodeVersion = tasks.shared.CodeVersion _

  type ResourceRequest = tasks.shared.VersionedResourceRequest

  def ResourceRequest(cpu: (Int, Int), memory: Int, scratch: Int)(
      implicit codeVersion: CodeVersion) =
    tasks.shared.VersionedResourceRequest(
      codeVersion,
      tasks.shared.ResourceRequest(cpu, memory, scratch))

  def ResourceRequest(cpu: Int, memory: Int, scratch: Int)(
      implicit codeVersion: CodeVersion) =
    tasks.shared.VersionedResourceRequest(
      codeVersion,
      tasks.shared.ResourceRequest(cpu, memory, scratch))

  implicit def tsc(implicit ts: TaskSystem): TaskSystemComponents =
    ts.components

  implicit def tasksConfig(
      implicit component: TaskSystemComponents): TasksConfig =
    component.tasksConfig

  implicit def codeVersionFromTasksConfig(
      implicit c: TasksConfig): CodeVersion = c.codeVersion

  implicit def fs(
      implicit component: TaskSystemComponents): FileServiceComponent =
    component.fs

  implicit def executionContext(
      implicit env: ComputationEnvironment): ExecutionContext =
    env.executionContext

  def releaseResources(implicit comp: ComputationEnvironment) =
    comp.launcher.actor.!(Release)(comp.taskActor)

  implicit def ts(
      implicit component: ComputationEnvironment): TaskSystemComponents =
    component.components

  implicit def launcherActor(
      implicit component: ComputationEnvironment): LauncherActor =
    component.launcher

  implicit def resourceAllocated(implicit component: ComputationEnvironment) =
    component.resourceAllocated

  implicit def log(implicit component: ComputationEnvironment) = component.log

  def withTaskSystem[T](f: TaskSystemComponents => T): Option[T] =
    withTaskSystem(None)(f)

  def withTaskSystem[T](c: Config)(f: TaskSystemComponents => T): Option[T] =
    withTaskSystem(Some(c))(f)

  def withTaskSystem[T](s: String)(f: TaskSystemComponents => T): Option[T] =
    withTaskSystem(Some(ConfigFactory.parseString(s)))(f)

  def withTaskSystem[T](c: Option[Config])(
      f: TaskSystemComponents => T): Option[T] = {
    val ts = defaultTaskSystem(c)
    if (ts.hostConfig.myRoles.contains(App)) {
      try {
        Some(f(ts.components))
      } finally {
        ts.shutdown
      }
    } else None

  }

  def defaultTaskSystem: TaskSystem =
    defaultTaskSystem(None)

  def defaultTaskSystem(string: String): TaskSystem =
    defaultTaskSystem(Some(ConfigFactory.parseString(string)))

  def defaultTaskSystem(extraConf: Option[Config]): TaskSystem = {

    val configuration =
      extraConf.map { extraConf =>
        ConfigFactory.defaultOverrides
          .withFallback(extraConf)
          .withFallback(ConfigFactory.load)
      } getOrElse
        ConfigFactory.load

    implicit val tconfig = tasks.util.config.parse(configuration)

    val elasticSupport = elastic.makeElasticSupport

    val hostConfig = elasticSupport
      .flatMap(_.hostConfig)
      .getOrElse(MasterSlaveGridEngineChosenFromConfig)

    val finalAkkaConfiguration = {

      val actorProvider = hostConfig match {
        case _: LocalConfiguration => "akka.actor.LocalActorRefProvider"
        case _                     => "akka.remote.RemoteActorRefProvider"
      }

      val numberOfAkkaRemotingThreads =
        if (hostConfig.myRoles.contains(Queue)) 6 else 2

      val akkaProgrammaticalConfiguration = ConfigFactory.parseString(s"""
        task-worker-dispatcher.fork-join-executor.parallelism-max = ${hostConfig.availableCPU}
        task-worker-dispatcher.fork-join-executor.parallelism-min = ${hostConfig.availableCPU}
        akka {
          actor {
            provider = "${actorProvider}"
          }
          remote {
            enabled-transports = ["akka.remote.netty.tcp"]
            netty.tcp {
              hostname = "${hostConfig.myAddress.getHostName}"
              port = ${hostConfig.myAddress.getPort.toString}
              server-socket-worker-pool.pool-size-max = ${numberOfAkkaRemotingThreads}
              client-socket-worker-pool.pool-size-max = ${numberOfAkkaRemotingThreads}
            }
         }
        }
          """)

      ConfigFactory.defaultOverrides
        .withFallback(akkaProgrammaticalConfiguration)
        .withFallback(ConfigFactory.parseResources("akka.conf"))
        .withFallback(configuration)

    }

    val system = ActorSystem(tconfig.actorSystemName, finalAkkaConfiguration)

    new TaskSystem(hostConfig, system, elasticSupport)
  }

  type CompFun[A, B] = A => ComputationEnvironment => B

  def AsyncTask[A <: AnyRef, C](taskID: String, taskVersion: Int)(
      comp: CompFun[A, Future[C]]): TaskDefinition[A, C] =
    macro Macros
      .asyncTaskDefinitionImpl[A, C]

  def MasterSlaveGridEngineChosenFromConfig(
      implicit config: TasksConfig): HostConfiguration =
    if (config.disableRemoting) new LocalConfigurationFromConfig
    else new MasterSlaveFromConfig

  def appendToFilePrefix[T](elements: Seq[String])(
      implicit ce: ComputationEnvironment): (ComputationEnvironment => T) => T =
    ce.withFilePrefix[T](elements) _

}
