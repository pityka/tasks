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

import scala.util._
import scala.concurrent._
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString

import tasks.queue._
import tasks._
import tasks.util.config._
import tasks.fileservice.{
  FileServiceComponent,
  FileServicePrefix,
  SharedFileHelper
}

private[tasks] class SharedFileCache(
    implicit fileServiceComponent: FileServiceComponent,
    AS: ActorSystem,
    EC: ExecutionContext,
    MAT: Materializer,
    config: TasksConfig)
    extends Cache
    with TaskSerializer {

  private val logger = akka.event.Logging(AS, getClass)

  override def toString = "SharedFileCache"

  def shutDown() = ()

  def get(taskDescription: TaskDescription)(
      implicit prefix: FileServicePrefix): Future[Option[UntypedResult]] = {

    val hash = SerializedTaskDescription(taskDescription).hash.hash
    val fileName = "__meta__result__" + hash
    SharedFileHelper
      .getByName(fileName, retrieveSizeAndHash = false)
      .flatMap {
        case None =>
          logger.debug(s"Not found $prefix $fileName for $taskDescription")
          Future.successful(None)
        case Some(sf) =>
          SharedFileHelper
            .getSourceToFile(sf, fromOffset = 0L)
            .runFold(ByteString.empty)(_ ++ _)
            .map { byteString =>
              val t = Try(deserializeResult(byteString.toArray))
              t.failed.foreach {
                case e: Exception =>
                  logger.debug(s"Failed to deserialize due to $e")
              }
              t.toOption
            }
            .recover {
              case e =>
                logger.error(e,
                             s"Failed to locate cached result file: $fileName")
                None
            }
      }

  }

  def set(taskDescription: TaskDescription, untypedResult: UntypedResult)(
      implicit p: FileServicePrefix) = {
    try {
      implicit val historyContext = tasks.fileservice.NoHistory
      val serializedTaskDescription = SerializedTaskDescription(taskDescription)
      val hash = serializedTaskDescription.hash.hash
      val value = serializeResult(untypedResult)
      val key = serializedTaskDescription.value
      for {
        _ <- SharedFileHelper
          .createFromSource(Source.single(ByteString(value)),
                            name = "__meta__result__" + hash)
        _ <- if (config.saveTaskDescriptionInCache)
          SharedFileHelper
            .createFromSource(Source.single(ByteString(key)),
                              name = "__meta__input__" + hash)
        else Future.successful(())
      } yield ()

    } catch {
      case e: Throwable => {
        shutDown
        throw e
      }
    }
  }

}
