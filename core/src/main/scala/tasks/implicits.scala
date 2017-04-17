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

import java.io.File
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
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

private[tasks] object Implicits {

  implicit def executionContext(
      implicit component: TaskSystemComponents): ExecutionContext =
    component.executionContext

  implicit def actorsystem(
      implicit component: TaskSystemComponents): ActorSystem =
    component.actorsystem

  implicit def actormaterializer(
      implicit component: TaskSystemComponents): ActorMaterializer =
    component.actorMaterializer

  implicit def filePrefix(
      implicit component: TaskSystemComponents): FileServicePrefix =
    component.filePrefix

  implicit def nodeLocalCache(
      implicit component: TaskSystemComponents): NodeLocalCacheActor =
    component.nodeLocalCache

  implicit def queueActor(
      implicit component: TaskSystemComponents): QueueActor = component.queue

  implicit def cacheActor(
      implicit component: TaskSystemComponents): CacheActor = component.cache

}
