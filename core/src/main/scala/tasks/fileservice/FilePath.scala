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

package tasks.fileservice

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

case class FileServicePrefix(list: Vector[String]) {
  def append(n: String) = FileServicePrefix(list :+ n)
  def append(ns: Seq[String]) = FileServicePrefix(list ++ ns)
  private[fileservice] def propose(name: String) =
    ProposedManagedFilePath(list :+ name)
}
object FileServicePrefix {
  implicit def toLogFeature(rm: FileServicePrefix): scribe.LogFeature =
    scribe.data(
      Map(
        "fileprefix" -> rm.list.mkString("/")
      )
    )
  implicit val codec: JsonValueCodec[FileServicePrefix] = JsonCodecMaker.make

}

case class ProposedManagedFilePath(list: Vector[String]) {
  def name = list.last
  def toManaged = ManagedFilePath(list)
}

object ProposedManagedFilePath {
  implicit def toLogFeature(rm: ProposedManagedFilePath): scribe.LogFeature =
    scribe.data(
      Map(
        "proposed-managed-file-path-uri" -> rm.list.mkString("/")
      )
    )
  implicit val codec: JsonValueCodec[ProposedManagedFilePath] =
    JsonCodecMaker.make
}
