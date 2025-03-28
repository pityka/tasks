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

import tasks.queue._

import tasks.fileservice.FileServicePrefix
import cats.effect.IO

private[tasks] abstract class Cache {

  def get(x: HashedTaskDescription)(implicit
      p: FileServicePrefix
  ): IO[Option[UntypedResult]]

  def set(x: HashedTaskDescription, r: UntypedResult)(implicit
      p: FileServicePrefix
  ): IO[Unit]

  def shutDown(): Unit

}

private[tasks] class DisabledCache extends Cache {

  def get(x: HashedTaskDescription)(implicit p: FileServicePrefix) =
    IO.pure(None)

  def set(x: HashedTaskDescription, r: UntypedResult)(implicit
      p: FileServicePrefix
  ) =
    IO.pure(())

  def shutDown() = {}

}
