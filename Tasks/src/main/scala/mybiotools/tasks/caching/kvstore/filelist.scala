/*
* The MIT License
*
* Copyright (c) 2016 Istvan Bartha
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

package tasks.caching.kvstore

import upickle.default._
import upickle.Js
import java.io.File

import tasks._
import tasks.fileservice._

class FileList(kv: KVStore) {

  implicit val fpWriter = upickle.default.Writer[FilePath] {
    case ManagedFilePath(elems) => Js.Obj("type" -> Js.Str("managed"), "elems" -> Js.Arr(elems.map(x => Js.Str(x)): _*))
    case RemoteFilePath(url) => Js.Obj("type" -> Js.Str("remote"), "url" -> Js.Str(url.toString))
  }
  implicit val fpReader = upickle.default.Reader[FilePath] {
    case Js.Obj(v @ _*) => {
      val map = v.toMap
      map("type") match {
        case Js.Str("remote") => RemoteFilePath(new java.net.URL(map("url").str))
        case Js.Str("managed") => ManagedFilePath(Vector(map("elems").arr.map(_.str): _*))
      }
    }
  }

  implicit val urlWriter = upickle.default.Writer[java.net.URL] {
    case t => Js.Str(t.toString)
  }
  implicit val urlReader = upickle.default.Reader[java.net.URL] {
    case Js.Str(t) => new java.net.URL(t)
  }

  implicit val fileWriter = upickle.default.Writer[java.io.File] {
    case t => Js.Str(t.toString)
  }
  implicit val fileReader = upickle.default.Reader[java.io.File] {
    case Js.Str(t) => new java.io.File(t)
  }

  implicit val sfWriter = upickle.default.Writer[SharedFile] {
    case t => Js.Obj(
      "size" -> Js.Str(t.byteSize.toString),
      "hash" -> Js.Str(t.hash.toString),
      "path" -> implicitly[Writer[FilePath]].write(t.path)
    )
  }
  implicit val sfReader = upickle.default.Reader[SharedFile] {
    case Js.Obj(v @ _*) => {
      val map = v.toMap
      new SharedFile(
        implicitly[Reader[FilePath]].read(map("path")),
        map("byteSize").str.toLong,
        map("hash").str.toInt
      )
    }
  }

  def get(sf: SharedFile): List[File] = kv.get(write(sf).getBytes).toList.flatMap(a => read[List[File]](new String(a)))
  def set(sf: SharedFile, l: List[File]): Unit = kv.put(write(sf).getBytes, write(l).getBytes)

}
