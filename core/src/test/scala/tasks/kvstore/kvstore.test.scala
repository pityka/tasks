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

package tasks.kvstore

import org.scalatest._
import tasks.util._

import java.io._

import org.scalatest.FunSpec
import org.scalatest.Matchers

import tasks.queue._
import tasks.caching._
import tasks.caching.kvstore._
import tasks.fileservice._
import tasks.util._
import tasks.shared._
import tasks.simpletask._
import tasks._

class LevelDBWrapperSpec extends KeyValueStoreSpec {
  def makeKVStore(f: File) = new LevelDBWrapper(f)
}

// class FileSystemKVStoreSpec extends KeyValueStoreSpec {
//   def makeKVStore(f: File) = new FileSystemLargeKVStore(f)
// }

trait KeyValueStoreSpec extends FunSpec with Matchers {

  def makeKVStore(f: File): KVStore

  describe(this.toString) {
    it("simple") {
      val tmp = TempFile.createTempFile(".1leveldb")
      tmp.delete
      val lw = makeKVStore(tmp)
      lw.put(Array(0, 1, 3), Array(0, 1, 3))
      lw.get(Array(0, 1, 3)).get.deep should equal(Array(0, 1, 3).deep)
      lw.close
    }
    it("simple 2") {
      val tmp = TempFile.createTempFile(".2leveldb")
      tmp.delete
      val lw = makeKVStore(tmp)
      lw.put(Array(0, 1, 3), Array(0, 1, 3))
      lw.get(Array(0, 1, 3)).get.deep should equal(Array(0, 1, 3).deep)
      lw.put(Array(0, 1, 3), Array(0, 1, 3))
      lw.get(Array(0, 1, 3)).get.deep should equal(Array(0, 1, 3).deep)
      lw.put(Array(0, 1, 4), Array(0, 1, 3))
      lw.put(Array(0, 1, 5), Array(0, 1, 3))
      lw.put(Array(0, 1, 6), Array(0, 1, 6))
      lw.put(Array(0, 1, 7), Array(0, 1, 6))
      lw.put(Array(0, 1, 7), Array(0, 1, 8))
      lw.get(Array(0, 1, 4)).get.deep should equal(Array(0, 1, 3).deep)
      lw.get(Array(0, 1, 5)).get.deep should equal(Array(0, 1, 3).deep)
      lw.get(Array(0, 1, 6)).get.deep should equal(Array(0, 1, 6).deep)
      lw.get(Array(0, 1, 7)).get.deep should equal(Array(0, 1, 8).deep)
      lw.close
    }
    it("overwrite") {
      val tmp = TempFile.createTempFile(".3leveldb")
      tmp.delete
      val lw = makeKVStore(tmp)
      lw.put(Array(0, 1, 3), Array(0, 1, 3))
      lw.get(Array(0, 1, 3)).get.deep should equal(Array(0, 1, 3).deep)
      lw.put(Array(0, 1, 3), Array(0, 1, 2))
      lw.get(Array(0, 1, 3)).get.deep should equal(Array(0, 1, 2).deep)
      lw.close
    }
    it("big") {
      val tmp = TempFile.createTempFile(".4leveldb")
      try {
        tmp.delete
        val data = Array.fill[Byte](1E6.toInt)(0)
        val lw = makeKVStore(tmp)
        0 until 100 foreach { i =>
          lw.put(java.nio.ByteBuffer.allocate(4).putInt(i).array, data)
        }
        lw.close
        val lw2 = makeKVStore(tmp)
        0 until 100 foreach { i =>
          assert(
              lw2
                .get(java.nio.ByteBuffer.allocate(4).putInt(i).array)
                .get
                .deep == (data.deep))
        }
        100 until 200 foreach { i =>
          lw2.put(java.nio.ByteBuffer.allocate(4).putInt(i).array, data)
        }
        lw2.close
        val lw3 = makeKVStore(tmp)
        0 until 200 foreach { i =>
          assert(
              lw3
                .get(java.nio.ByteBuffer.allocate(4).putInt(i).array)
                .get
                .deep == (data.deep))
        }
        lw3.close
      } finally {
        tmp.listFiles.map(_.delete)
      }

    }
  }

}
