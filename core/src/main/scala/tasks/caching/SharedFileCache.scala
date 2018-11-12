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
import akka.stream.Materializer

import tasks.queue._
import tasks._
import tasks.util._
import tasks.util.config._
import tasks.fileservice.{
  FileServiceComponent,
  FileServicePrefix,
  SharedFileHelper
}

import com.google.common.hash.Hashing

private[tasks] class SharedFileCache(
    implicit fileServiceComponent: FileServiceComponent,
    nodeLocalCacheActor: NodeLocalCacheActor,
    AS: ActorSystem,
    EC: ExecutionContext,
    MAT: Materializer,
    config: TasksConfig)
    extends Cache
    with TaskSerializer {

  private def getHash(a: Array[Byte]): String =
    Hashing.murmur3_128.hashBytes(a).toString

  private val logger = akka.event.Logging(AS, getClass)

  override def toString = "SharedFileCache"

  def shutDown() = ()

  def get(taskDescription: TaskDescription)(
      implicit prefix: FileServicePrefix): Future[Option[UntypedResult]] = {
    val taskDescriptionBytes = serializeTaskDescription(taskDescription)
    val hash = getHash(taskDescriptionBytes)
    val fileName = "__meta__result__" + hash
    SharedFileHelper
      .getByNameUnchecked(fileName)
      .flatMap {
        case None     => Future.successful(None)
        case Some(sf) => SharedFileHelper.getPathToFile(sf).map(Some(_))
      }
      .recover {
        case e =>
          logger.error(e, s"Failed to locate cached result file: $fileName")
          None
      }
      .map { file =>
        file.flatMap { file =>
          logger.debug(s"Looking for result description $file")
          val t = Try(deserializeResult(readBinaryFile(file)))
          t.failed.foreach {
            case e: java.io.IOException =>
              logger.debug(s"Not found $file. $e")
            case e: Exception =>
              logger.debug(s"Failed to deserialize due to $e")
          }
          t.toOption
        }
      }
  }

  def set(taskDescription: TaskDescription, untypedResult: UntypedResult)(
      implicit p: FileServicePrefix) = {
    try {
      implicit val historyContext = tasks.fileservice.NoHistory
      val serializedTaskDescription = serializeTaskDescription(taskDescription)
      val hash = getHash(serializedTaskDescription)
      val value: File = writeBinaryToFile(serializeResult(untypedResult))
      val key: File = writeBinaryToFile(serializedTaskDescription)
      for {
        _ <- SharedFileHelper
          .createFromFile(value,
                          name = "__meta__result__" + hash,
                          deleteFile = true)
        _ <- SharedFileHelper
          .createFromFile(key,
                          name = "__meta__input__" + hash,
                          deleteFile = true)
      } yield ()

    } catch {
      case e: Throwable => {
        shutDown
        throw e
      }
    }
  }

}
