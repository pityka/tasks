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

import java.io.File
import akka.actor.{ActorRef, ActorSystem}
import scala.concurrent.Future
import akka.event.LogSource
import java.util.concurrent.TimeUnit.{MILLISECONDS, NANOSECONDS, SECONDS}

import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext

import scala.collection.JavaConversions._
import scala.util.Try

import akka.actor.{Props, ActorRefFactory, ActorContext}
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration._
import scala.concurrent._
import akka.pattern.ask

import tasks.caching._
import tasks.queue._
import tasks.fileservice._
import tasks.util._
import tasks.deploy._
import tasks.shared._
import tasks.elastic.ec2._
import tasks.elastic.ssh._

import upickle.default._
import upickle.Js

import scala.language.experimental.macros

package object tasks {

  val SharedFile = tasks.fileservice.SharedFile

  type SharedFile = tasks.fileservice.SharedFile

  type CPUMemoryRequest = tasks.shared.CPUMemoryRequest

  def CPUMemoryRequest(cpu: (Int, Int), memory: Int) =
    tasks.shared.CPUMemoryRequest(cpu, memory)
  def CPUMemoryRequest(cpu: Int, memory: Int) =
    tasks.shared.CPUMemoryRequest(cpu, memory)

  implicit def tsc(implicit ts: TaskSystem): TaskSystemComponents =
    ts.components

  implicit def fs(implicit component: TaskSystemComponents): FileServiceActor =
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

  def remoteCacheAddress(implicit t: QueueActor, s: ActorContext): String =
    if (t.actor.path.address.host.isDefined && t.actor.path.address.port.isDefined)
      t.actor.path.address.host.get + ":" + t.actor.path.address.port.get
    else
      s.system.settings.config
        .getString("akka.remote.netty.tcp.hostname") + ":" + s.system.settings.config
        .getString("akka.remote.netty.tcp.port")

  def withTaskSystem[T](f: TaskSystemComponents => T): Option[T] =
    withTaskSystem(None)(f)

  def withTaskSystem[T](c: Option[Config])(
      f: TaskSystemComponents => T): Option[T] = {
    val ts = defaultTaskSystem(c)
    if (ts.hostConfig.myRole == MASTER) {
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

    val hostConfig = MasterSlaveGridEngineChosenFromConfig

    val configuration = {
      val actorProvider = hostConfig match {
        case x: LocalConfiguration => "akka.actor.LocalActorRefProvider"
        case _ => "akka.remote.RemoteActorRefProvider"
      }

      val numberOfAkkaRemotingThreads =
        if (hostConfig.myRole == MASTER) 6 else 2

      val configSource = s"""
          task-worker-dispatcher.fork-join-executor.parallelism-max = ${hostConfig.myCardinality}
          task-worker-dispatcher.fork-join-executor.parallelism-min = ${hostConfig.myCardinality}
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
            """ + (if (tasks.util.config.global.logToStandardOutput)
                     """
              akka.loggers = ["akka.event.Logging$DefaultLogger"]
              """
                   else "")

      val customConf = ConfigFactory.parseString(configSource)

      val akkaconf = ConfigFactory.parseResources("akkaoverrides.conf")

      extraConf.map { extraConf =>
        ConfigFactory.defaultOverrides
          .withFallback(customConf)
          .withFallback(extraConf)
          .withFallback(akkaconf)
          .withFallback(ConfigFactory.load)
      } getOrElse (
          ConfigFactory.defaultOverrides
            .withFallback(customConf)
            .withFallback(akkaconf)
            .withFallback(ConfigFactory.load)
      )

    }

    val system = ActorSystem("tasks", configuration)
    new TaskSystem(MasterSlaveGridEngineChosenFromConfig, system)
  }

  def defaultTaskSystem(as: ActorSystem): TaskSystem =
    new TaskSystem(MasterSlaveGridEngineChosenFromConfig, as)

  def customTaskSystem(hostConfig: MasterSlaveConfiguration,
                       extraConf: Config): TaskSystem = {
    val akkaconf = ConfigFactory.parseResources("akkaoverrides.conf")

    val conf = ConfigFactory.defaultOverrides
      .withFallback(extraConf)
      .withFallback(akkaconf)
      .withFallback(ConfigFactory.defaultReference)

    val system = ActorSystem("tasks", conf)

    new TaskSystem(hostConfig, system)
  }

  def customTaskSystem(hostConfig: MasterSlaveConfiguration,
                       as: ActorSystem): TaskSystem =
    new TaskSystem(hostConfig, as)

  type CompFun[A, B] = A => ComputationEnvironment => B

  def AsyncTask[A, C](taskID: String, taskVersion: Int)(
      comp: CompFun[A, Future[C]]): TaskDefinition[A, C] = macro Macros
    .asyncTaskDefinitionImpl[A, C]

  def Task[A, C](taskID: String, taskVersion: Int)(
      comp: CompFun[A, C]): TaskDefinition[A, C] = macro Macros
    .taskDefinitionImpl[A, C]

  def MasterSlaveGridEngineChosenFromConfig: MasterSlaveConfiguration = {
    if (config.global.disableRemoting) LocalConfigurationFromConfig
    else
      config.global.gridEngine match {
        case x if x == "EC2" => EC2MasterSlave
        case x if x == "NOENGINE" => MasterSlaveFromConfig
        case _ => MasterSlaveFromConfig
      }
  }

}
