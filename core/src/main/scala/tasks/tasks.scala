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

import tasks.queue._
import tasks.shared.{Priority, Labels}
import scala.concurrent._
import cats.effect.IO
import cats.effect.kernel.Deferred

trait HasSharedFiles extends Product {
  private[tasks] def immutableFiles: Seq[SharedFile]
  private[tasks] def mutableFiles: Seq[SharedFile]
  private[tasks] def allFiles: Seq[SharedFile] = immutableFiles ++ mutableFiles
}

object HasSharedFiles {
  def allFiles(r: Any): Set[SharedFile] = recurse(r)(_.allFiles).toSet

  private[tasks] def recurse(
      a: Any
  )(f: HasSharedFiles => Seq[SharedFile]): Seq[SharedFile] = a match {
    case t: HasSharedFiles => f(t)
    case t: Iterable[_]    => t.flatMap(r => recurse(r)(f)).toSeq
    case t: Product => t.productIterator.flatMap(r => recurse(r)(f)).toSeq
    case _          => Nil
  }

}

/* Marker trait for case classes with SharedFile members
 *
 * Discovered SharedFile members are verified after the object is recovered from the cache.
 * Discovered SharedFile members are also used to trace data dependencies between tasks.
 *
 * Classes extending this abstract class have to list all their members as follows.
 * - `mutables` argument list forces all files listed there to be treated as mutable
 * - non autodiscoverable HasSharedFiles should be listed in `members`
 * - direct members, instances of Traversable or Product are automatically discovered
 *   with simple runtime type checks (without reflection)
 *
 * See the WithSharedFilesTestSuite.scala for examples
 */
abstract class WithSharedFiles(
    members: Seq[Any] = Nil,
    mutables: Seq[Any] = Nil
) extends Product
    with HasSharedFiles {

  import HasSharedFiles.recurse

  private[tasks] def immutableFiles = {
    val mutables = mutableFiles.toSet
    (members
      .flatMap(m => recurse(m)(_.immutableFiles)) ++ this.productIterator
      .flatMap(p => recurse(p)(_.immutableFiles))).distinct.filterNot(mutables)
  }

  private[tasks] def mutableFiles =
    (members
      .flatMap(m => recurse(m)(_.mutableFiles)) ++ this.productIterator.flatMap(
      p => recurse(p)(_.mutableFiles)
    ) ++ mutables.flatMap(m => recurse(m)(_.allFiles))).distinct
}

trait HasPersistent[+A] { self: A =>
  def persistent: A
}

final class TaskDefinition[A: Serializer, B: Deserializer](
    private[tasks] val rs: Spore[Unit, Deserializer[A]],
    private[tasks] val ws: Spore[Unit, Serializer[B]],
    private[tasks] val fs: Spore[A, ComputationEnvironment => IO[B]],
    private[tasks] val taskId: TaskId
) {

  def writer1 = implicitly[Serializer[A]]
  def reader2 = implicitly[Deserializer[B]]

  def apply(a: A)(
      resource: ResourceRequest,
      priorityBase: Priority = Priority(0),
      labels: Labels = Labels.empty,
      noCache: Boolean = false
  )(implicit components: TaskSystemComponents): IO[B] = {
    implicit val queue = components.queue
    implicit val cache = components.cache
    implicit val prefix = components.filePrefix

    val taskId1 = taskId

    Deferred[IO, Either[Throwable, B]].flatMap { deferred =>
      val behavior = new ProxyTask[A, B](
        taskId = taskId1,
        inputDeserializer = rs,
        outputSerializer = ws,
        function = fs,
        input = a,
        writer = writer1,
        reader = reader2,
        resourceConsumed = resource,
        queue = queue,
        fileServicePrefix = prefix,
        cache = cache,
        priority = Priority(priorityBase.s + components.priority.s + 1),
        promise = deferred,
        labels = components.labels ++ labels,
        lineage = components.lineage,
        noCache = noCache,
        messenger = components.messenger
      )
      val r = tasks.util.Actor
        .makeFromBehavior[Proxy](behavior, components.messenger)
      r.use { case proxy =>
        deferred.get.flatMap { value =>
          IO.fromEither(value)
        }
      }
    }

  }

}
