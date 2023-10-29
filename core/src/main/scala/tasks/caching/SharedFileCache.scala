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

import akka.actor.ActorSystem

import tasks.queue._
import tasks._
import tasks.util.config._
import tasks.fileservice.{
  FileServiceComponent,
  FileServicePrefix,
  SharedFileHelper
}
import cats.effect.IO
import fs2.Chunk

private[tasks] class SharedFileCache(implicit
    fileServiceComponent: FileServiceComponent,
    config: TasksConfig
) extends Cache
    with TaskSerializer {

  override def toString = "SharedFileCache"

  def shutDown() = ()

  def get(
      hashedTaskDescription: HashedTaskDescription
  )(implicit prefix: FileServicePrefix): IO[Option[UntypedResult]] = {

    val hash = hashedTaskDescription.hash
    val fileName = "__meta__result__" + hash
    SharedFileHelper
      .getByName(fileName, retrieveSizeAndHash = false)
      .flatMap {
        case None =>
          scribe.debug(
            s"Not found $prefix $fileName for $hashedTaskDescription"
          )
          IO.pure(None)
        case Some(sf) =>
          SharedFileHelper
            .stream(sf, fromOffset = 0L)
            .compile
            .foldChunks(Chunk.empty[Byte])(_ ++ _)
            .map { byteString =>
              val t = deserializeResult(byteString.toArray)
              Some(t)
            }
            .handleError { case e =>
              scribe.error(
                e,
                s"Failed to locate cached result file: $fileName"
              )
              None
            }
      }

  }

  def set(
      hashedTaskDescription: HashedTaskDescription,
      untypedResult: UntypedResult
  )(implicit
      p: FileServicePrefix
  ) = {
    try {
      implicit val historyContext = tasks.fileservice.NoHistory
      val hash = hashedTaskDescription.hash
      val value = serializeResult(untypedResult)
      for {
        _ <- SharedFileHelper
          .createFromStream(
            fs2.Stream.chunk(Chunk.array(value)),
            name = "__meta__result__" + hash
          )

      } yield ()

    } catch {
      case e: Throwable => {
        shutDown()
        throw e
      }
    }
  }

}
