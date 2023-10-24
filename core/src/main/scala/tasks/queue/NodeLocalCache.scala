/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
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

package tasks.queue

import tasks.TaskSystemComponents
import cats.effect.kernel.Resource
import cats.effect.IO

object NodeLocalCache {
  type State = tasks.util.concurrent.NodeLocalCache.StateR[Any]
  private[tasks] def start =
    tasks.util.concurrent.NodeLocalCache.make[Any]

  def resource[A](key: String, resource: Resource[IO, A])(implicit
      tsc: TaskSystemComponents
  ): Resource[IO, A] =
    tasks.util.concurrent.NodeLocalCache
      .offer(key, resource, tsc.nodeLocalCache)
      .asInstanceOf[Resource[IO, A]]

  def cacheSyncWithRelease[A](key: String, orElse: => A)(
      release: A => IO[Unit]
  )(implicit tsc: TaskSystemComponents): Resource[IO, A] =
    tasks.util.concurrent.NodeLocalCache
      .offer(key, Resource.make(IO(orElse))(release), tsc.nodeLocalCache)
      .asInstanceOf[Resource[IO, A]]

  def cacheAsyncWithRelease[A](key: String, orElse: => IO[A])(
      release: A => IO[Unit]
  )(implicit tsc: TaskSystemComponents): Resource[IO, A] =
    tasks.util.concurrent.NodeLocalCache
      .offer(
        key,
        Resource.make(orElse)(release),
        tsc.nodeLocalCache
      )
      .asInstanceOf[Resource[IO, A]]

  def cacheSync[A](key: String, orElse: => A)(implicit
      tsc: TaskSystemComponents
  ) =
    tasks.util.concurrent.NodeLocalCache
      .offer(key, Resource.make(IO(orElse))(_ => IO.unit), tsc.nodeLocalCache)
      .asInstanceOf[Resource[IO, A]]

  def cacheAsync[A](key: String, orElse: => IO[A])(implicit
      tsc: TaskSystemComponents
  ): Resource[IO, A] =
    tasks.util.concurrent.NodeLocalCache
      .offer(
        key,
        Resource.make(orElse)(_ => IO.unit),
        tsc.nodeLocalCache
      )
      .asInstanceOf[Resource[IO, A]]

  def cacheIO[A](key: String, orElse: => IO[A])(implicit
      tsc: TaskSystemComponents
  ): Resource[IO, A] =
    tasks.util.concurrent.NodeLocalCache
      .offer(key, Resource.make(orElse)(_ => IO.unit), tsc.nodeLocalCache)
      .asInstanceOf[Resource[IO, A]]

}
