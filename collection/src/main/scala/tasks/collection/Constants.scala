package tasks.ecoll

import akka.util.ByteString

trait Constants {
  val ElemBufferSize = 256
  val BufferSize = 1024L * 512
  protected val Eol = ByteString("\n")

}
