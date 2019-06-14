package tasks.ecoll

import flatjoin._
import tasks.queue._
import java.nio._
import tasks.util.rightOrThrow

trait FlatjoinSupport {

  implicit def flatJoinFormat[T: Deserializer: Serializer] = new Format[T] {
    def toBytes(t: T): ByteBuffer =
      ByteBuffer.wrap(implicitly[Serializer[T]].apply(t))
    def fromBytes(bb: ByteBuffer): T = {
      val ba = ByteBuffer.allocate(bb.remaining)
      while (ba.hasRemaining) {
        ba.put(bb.get)
      }
      rightOrThrow(implicitly[Deserializer[T]].apply(ba.array))
    }
  }
}
