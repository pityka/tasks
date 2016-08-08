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

import tasks.util._
import tasks._
import com.google.common.io.Files
import com.google.common.io.BaseEncoding
import java.io.File
import scala.collection.JavaConversions._

sealed class FileSystemLargeKVStore(file: File) extends LargeKeyKVStore(new FileSystemKVStore(file))

sealed class FileSystemKVStore(file: File) extends KVStoreWithDelete {

  private val leveldb = {
    if (file.isFile) {
      throw new RuntimeException("File exists: " + file.getAbsolutePath)
    } else if (!file.isDirectory) {
      file.mkdirs
    }
  }

  private def encode(a: Array[Byte]): String = BaseEncoding.base32().encode(a)
  private def decode(s: String): Array[Byte] = BaseEncoding.base32().decode(s)
  private def makeFile(key: Array[Byte]) = new File(file, encode(key))

  def listKeys = file.listFiles.toList.map(f => Files.asByteSource(f).read)

  def close: Unit = {}

  def put(k: Array[Byte], v: Array[Byte]): Unit = {
    Files.write(v, makeFile(k))
  }

  def get(k: Array[Byte]): Option[Array[Byte]] = {
    val f = makeFile(k)
    if (!f.canRead) None
    else Some(Files.asByteSource(f).read)
  }

  def delete(k: Array[Byte]): Unit = makeFile(k).delete
}
