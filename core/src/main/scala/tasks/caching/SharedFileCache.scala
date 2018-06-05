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
import scala.util._
import scala.concurrent._
import akka.actor.ActorSystem

import tasks.queue._
import tasks._
import tasks.util._
import tasks.util.config._
import tasks.fileservice.{FileServiceActor, FileServicePrefix, SharedFileHelper}

import com.google.common.hash.Hashing

private[tasks] class SharedFileCache(implicit fs: FileServiceActor,
                                     nlc: NodeLocalCacheActor,
                                     as: ActorSystem,
                                     ec: ExecutionContext,
                                     config: TasksConfig)
    extends Cache
    with TaskSerializer {

  def getHash(a: Array[Byte]): String =
    Hashing.murmur3_128.hashBytes(a).toString

  private val logger = akka.event.Logging(as, getClass)

  override def toString = "SharedFileCache"

  def shutDown() = ()

  def get(x: TaskDescription)(
      implicit p: FileServicePrefix): Future[Option[UntypedResult]] = {
    val tdBytes = serializeTaskDescription(x)
    val hash = getHash(tdBytes)
    SharedFileHelper
      .getByNameUnchecked("__meta__result__" + hash)
      .flatMap(sf => SharedFileHelper.getPathToFile(sf))
      .map(x => Some(x))
      .recover {
        case e =>
          println(e)
          None
      }
      .map { f =>
        f.flatMap { f =>
          logger.debug("Reading result " + f)
          Try(deserializeResult(readBinaryFile(f))).toOption
        }
      }
  }

  def set(x: TaskDescription, r: UntypedResult)(
      implicit p: FileServicePrefix) = {
    try {
      val std = serializeTaskDescription(x)
      val kh = getHash(std)
      val v: File = writeBinaryToFile(serializeResult(r))
      val k: File = writeBinaryToFile(std)
      SharedFileHelper
        .createFromFile(v, name = "__meta__result__" + kh, deleteFile = true)
        .flatMap { _ =>
          SharedFileHelper
            .createFromFile(k, name = "__meta__input__" + kh, deleteFile = true)
            .map { _ =>
              ()
            }
        }
    } catch {
      case e: Throwable => {
        shutDown
        throw e
      }
    }
  }

}
