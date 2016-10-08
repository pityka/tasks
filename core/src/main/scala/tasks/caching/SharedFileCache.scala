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
import scala.util._
import akka.actor.ActorRefFactory

import tasks.queue._
import tasks._
import tasks.util._
import tasks.fileservice.{FileServiceActor, FileServicePrefix, SharedFileHelper}

import upickle.Js

import com.google.common.hash.Hashing

class SharedFileCache(implicit fs: FileServiceActor,
                      nlc: NodeLocalCacheActor,
                      af: ActorRefFactory)
    extends Cache
    with TaskSerializer {

  def getHash(a: Array[Byte]): String =
    Hashing.murmur3_128.hashBytes(a).toString

  override def toString = "SharedFileCache"

  def shutDown = ()

  def get(x: TaskDescription)(implicit p: FileServicePrefix) = {
    val tdBytes = serializeTaskDescription(x)
    val hash = getHash(tdBytes)
    val sf: Option[SharedFile] = Try(SharedFileHelper.getByNameUnchecked(
            "__result__" + hash)).toOption.flatten
    sf.flatMap { sf =>
      Try(deserializeResult(readBinaryFile(sf.file))).toOption
    }
  }

  def set(x: TaskDescription, r: UntypedResult)(
      implicit p: FileServicePrefix) = {
    try {
      val kh = getHash(serializeTaskDescription(x))
      val v: File = writeBinaryToFile(serializeResult(r))
      SharedFile(v, name = "__result__" + kh)
    } catch {
      case e: Throwable => {
        shutDown
        throw e
      }
    }
  }

}
