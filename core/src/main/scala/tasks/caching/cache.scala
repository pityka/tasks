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

package tasks.caching

import java.io.File
import java.io.FileInputStream
import java.io.{ObjectOutputStream, ObjectInputStream, FileOutputStream}
import scala.collection.immutable.ListMap
import scala.concurrent._

import kvstore._
import tasks.queue._
import tasks._

import tasks.fileservice.FileServicePrefix

import upickle.Js

abstract class Cache {

  def get(x: TaskDescription)(
      implicit p: FileServicePrefix): Future[Option[UntypedResult]]

  def set(x: TaskDescription, r: UntypedResult)(
      implicit p: FileServicePrefix): Future[Unit]

  def shutDown: Unit

}

object LevelDBCache {
  def apply(
      f: File,
      serialization: akka.serialization.Serialization
  ) =
    new KVCache(new LevelDBWrapper(f), serialization)
}

class KVCache(
    kvstore: KVStore,
    protected val serialization: akka.serialization.Serialization
) extends Cache
    with TaskSerializer {

  override def toString = "KVCacheWith" + kvstore

  def shutDown = kvstore.close

  def get(x: TaskDescription)(implicit p: FileServicePrefix) =
    Future.successful(kvstore.get(serializeTaskDescription(x)).flatMap { r =>
      scala.util.Try(deserializeResult(r)).toOption
    })

  def set(x: TaskDescription, r: UntypedResult)(
      implicit p: FileServicePrefix) = {
    try {
      val k: Array[Byte] = serializeTaskDescription(x)
      val v: Array[Byte] = serializeResult(r)
      kvstore.put(k, v)
      Future.successful(())
    } catch {
      case e: Throwable => {
        shutDown
        throw e
      }
    }
  }

}

class DisabledCache extends Cache {

  def get(x: TaskDescription)(implicit p: FileServicePrefix) =
    Future.successful(None)

  def set(x: TaskDescription, r: UntypedResult)(
      implicit p: FileServicePrefix) = Future.successful(())

  def shutDown = {}

}

class FakeCacheForTest extends Cache {

  var cacheMap: scala.collection.mutable.ListMap[TaskDescription,
                                                 UntypedResult] =
    scala.collection.mutable.ListMap[TaskDescription, UntypedResult]()

  def get(x: TaskDescription)(implicit p: FileServicePrefix) =
    Future.successful(cacheMap.get(x))

  def set(x: TaskDescription, r: UntypedResult)(
      implicit p: FileServicePrefix) =
    Future.successful(cacheMap.getOrElseUpdate(x, r))

  def shutDown = {}

}
