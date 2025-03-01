package tasks

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

import java.io.File

package object wire {

  implicit val throwableCodec: JsonValueCodec[Throwable] = {
    type DTO = (String, List[(String, String, String, Int)])
    val codec0: JsonValueCodec[DTO] = JsonCodecMaker.make

    new JsonValueCodec[Throwable] {
      val nullValue: Throwable = null.asInstanceOf[Throwable]

      def encodeValue(
          throwable: Throwable,
          out: JsonWriter
      ): _root_.scala.Unit = {
        val dto = (
          throwable.getMessage,
          throwable.getStackTrace.toList.map(stackTraceElement =>
            (
              stackTraceElement.getClassName,
              stackTraceElement.getMethodName,
              stackTraceElement.getFileName,
              stackTraceElement.getLineNumber
            )
          )
        )
        codec0.encodeValue(dto, out)
      }

      def decodeValue(in: JsonReader, default: Throwable): Throwable = {
        val (msg, stackTrace) = codec0.decodeValue(in, codec0.nullValue)
        val exc = new Exception(msg)
        exc.setStackTrace(stackTrace.map { case (cls, method, file, line) =>
          new java.lang.StackTraceElement(cls, method, file, line)
        }.toArray)
        exc
      }
    }
  }

  implicit val fileCodec: JsonValueCodec[File] = {
    implicit val codec0: JsonValueCodec[String] = JsonCodecMaker.make
    new JsonValueCodec[File] {
      val nullValue: File = null.asInstanceOf[File]

      def encodeValue(x: File, out: JsonWriter): _root_.scala.Unit = {
        val path = x.getAbsolutePath()
        codec0.encodeValue(path, out)
      }

      def decodeValue(in: JsonReader, default: File): File = {
        val dto = codec0.decodeValue(in, codec0.nullValue)
        new File(dto)
      }
    }
  }

}
