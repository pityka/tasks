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

import java.io.{File, FileOutputStream, DataOutputStream, FileInputStream, DataInputStream}
import collection.JavaConversions._
import java.io.{DataOutputStream, DataInputStream}
import com.google.common.hash._

trait KVStore {

  def close: Unit

  def put(k: Array[Byte], v: Array[Byte]): Unit

  def get(k: Array[Byte]): Option[Array[Byte]]

  def listKeys: List[Array[Byte]]

  def copyFrom(other: KVStore) = other.listKeys.foreach { k =>
    other.get(k).foreach { v =>
      put(k, v)
    }
  }

}

trait KVStoreWithDelete extends KVStore {
  def delete(k: Array[Byte]): Unit
}

/**
  * A key-value store which store big-keyed values in an other k-v store
  *
  * Both keys and values are stored as values in the underlying database
  * Values are stored with a |0|xxxxxxxx| where the last 8 bytes represent a Long
  * Keys are stored with |0|xxxxxxxxxxxxxxxx| where the last 16 bytes are the hash of the key
  * The stored values of the key under that hash is a list of |length|address|key| where length is the length of the 'key' and address is the Long address under which the value is stored
  * There is an extra key in the database, |0| which stores the highest Long address.
  */
abstract class LargeKeyKVStore(store: KVStoreWithDelete) extends KVStore {

  def close = store.close

  private var counter: Long = {
    val r = store.get(Array(0))
    if (r == None) 0L
    else java.nio.ByteBuffer.wrap(r.get).getLong
  }

  private val ValueKeyPrefix: Byte = 1
  private val KeyKeyPrefix: Byte = 2

  def listKeys = store.listKeys.filter(x => x.size > 0 && x.apply(0) == 2)

  private def getKeyHash(a: Array[Byte]): Array[Byte] = {
    {
      val h = Hashing.murmur3_128.hashBytes(a).asBytes
      val b = java.nio.ByteBuffer.allocate(1 + h.size)
      b.put(KeyKeyPrefix)
      b.put(h)
      b.array()
    }

  }

  private def getValueAddress(a: Long): Array[Byte] = {
    val b = java.nio.ByteBuffer.allocate(9)
    b.put(ValueKeyPrefix)
    b.putLong(a)
    b.array()
  }

  def get(key: Array[Byte]): Option[Array[Byte]] = {
    val keyhash = getKeyHash(key)
    val valuepairList = store.get(keyhash)
    if (valuepairList == None) None
    else {
      findKey(valuepairList.get, key).flatMap { counterAddress =>
        store.get(getValueAddress(counterAddress))
      }
    }
  }

  private def findKey(list: Array[Byte], key: Array[Byte]): Option[Long] = {
    var buffer = java.nio.ByteBuffer.wrap(list)
    var found = false
    var address = 0L
    while (!found && buffer.position < list.size) {
      val length = buffer.getInt
      address = buffer.getLong
      val ar = Array.ofDim[Byte](length)
      buffer.get(ar)
      if (java.util.Arrays.equals(ar, key)) {
        found = true
      }
    }
    if (found) Some(address) else None
  }

  private def replaceOrAppend(list: Array[Byte],
                              key: Array[Byte],
                              address: Long): Array[Byte] = {

    // |length|counter|bytes of key|
    val valuepairWithSize: Array[Byte] = {
      val b = java.nio.ByteBuffer.allocate(key.size + 12)
      b.putInt((key.size).toInt)
      b.putLong(address)
      b.put(key)
      b.array()
    }

    val bufferin = java.nio.ByteBuffer.wrap(list)
    val bufferout = java.nio.ByteBuffer.allocate(list.size)
    while (bufferin.position < list.size) {
      val length = bufferin.getInt
      val address = bufferin.getLong
      val ar = Array.ofDim[Byte](length)
      bufferin.get(ar)
      if (!java.util.Arrays.equals(ar, key)) {
        bufferout.putInt(length).putLong(address).put(ar)
      } else {
        store.delete(getValueAddress(address))
      }
    }

    {
      val ar = Array.ofDim[Byte](bufferout.position)
      bufferout.rewind
      bufferout.get(ar)
      ar
    } ++ valuepairWithSize

  }

  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    counter += 1
    store
      .put(Array(0), java.nio.ByteBuffer.allocate(8).putLong(counter).array())
    store.put(getValueAddress(counter), value)

    val keyhash: Array[Byte] = getKeyHash(key)

    val valuepairListIfCollision = store.get(keyhash)
    if (valuepairListIfCollision == None) {
      store.put(keyhash, replaceOrAppend(Array[Byte](), key, counter))
    } else {
      store.put(keyhash,
                replaceOrAppend(valuepairListIfCollision.get, key, counter))
    }

  }

}
