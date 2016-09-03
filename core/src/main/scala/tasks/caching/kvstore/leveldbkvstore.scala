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

package tasks.caching.kvstore

import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._

import java.io.{File, FileOutputStream, DataOutputStream, FileInputStream, DataInputStream}
import collection.JavaConversions._
import tasks.util._
import tasks._

sealed class LevelDBWrapper(file: File)
    extends LargeKeyKVStore(new DirectLevelDBWrapper(file))

sealed class DirectLevelDBWrapper(file: File) extends KVStoreWithDelete {

  private val leveldb = {
    if (file.isFile) {
      throw new RuntimeException("File exists: " + file.getAbsolutePath)
    } else if (!file.isDirectory) {
      file.mkdirs
    }
    val options = new Options();
    options.createIfMissing(true);
    options.maxOpenFiles(50);
    factory.open(file, options);
  }

  private val writeOption = {
    val o = new WriteOptions
    o.sync(true)
  }

  def listKeys = leveldb.iterator.toList.map(_.getKey)

  def close: Unit = leveldb.close

  def put(k: Array[Byte], v: Array[Byte]): Unit =
    leveldb.put(k, v, writeOption)

  def get(k: Array[Byte]): Option[Array[Byte]] = {
    val r = leveldb.get(k)
    if (r == null) None
    else Some(r)
  }

  def delete(k: Array[Byte]): Unit = leveldb.delete(k)
}
