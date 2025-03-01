/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
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

package tasks

import java.io.{
  PrintWriter,
  BufferedWriter,
  FileWriter,
  FileInputStream,
  FileOutputStream,
  BufferedOutputStream,
  BufferedInputStream,
  StringWriter,
  File
}

import scala.sys.process._
import scala.concurrent.duration._
import scala.util._
import tasks.util.config._
import cats.effect.IO
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

package object util {

  def base64(b: Array[Byte]): String =
    java.util.Base64.getEncoder.encodeToString(b)
  def base64(s: String): Array[Byte] = java.util.Base64.getDecoder.decode(s)

  private def available(port: Int): Boolean = {
    var s: java.net.Socket = null
    try {
      s = new java.net.Socket("localhost", port);

      false
    } catch {
      case _: Exception => true
    } finally {
      if (s != null) {

        s.close

      }
    }
  }

  def chooseNetworkPort(implicit config: TasksConfig): Int =
    Try(config.hostPort)
      .flatMap { p =>
        if (available(p)) Success(p) else Failure(new RuntimeException)
      }
      .getOrElse {
        if (config.mayUseArbitraryPort) {
        val s = new java.net.ServerSocket(0);
        val p = s.getLocalPort()
        s.close
        p
        } else throw new RuntimeException(s"Configured port ${config.hostPort} already taken. ")
      }

  def stackTraceAsString(t: Any): String = {
    if (t.isInstanceOf[Throwable]) {
      val sw = new StringWriter();
      val pw = new PrintWriter(sw);
      t.asInstanceOf[Throwable].printStackTrace(pw);
      sw.toString(); // stack trace as a string
    } else t.toString
  }

  def rethrow[T](
      messageOnError: => String,
      exceptionFactory: (=> String, Throwable) => Throwable
  )(block: => T): T =
    try {
      block
    } catch {
      case e: Throwable => throw (exceptionFactory(messageOnError, e))
    }

  def rethrow[T](messageOnError: => String)(block: => T): T =
    rethrow(messageOnError, new RuntimeException(_, _))(block)

  /** Retry the given block n times. */
  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): Try[T] =
    Try(fn) match {
      case x: Success[T] => x
      case _ if n > 1    => retry(n - 1)(fn)
      case f             => f
    }

  /** Returns the result of the block, and closes the resource.
    *
    * @param param
    *   closeable resource
    * @param f
    *   block using the resource
    */
  def useResource[A <: { def close(): Unit }, B](param: A)(f: A => B): B =
    try {
      f(param)
    } finally {
      import scala.language.reflectiveCalls
      param.close()
    }

 

  /** Writes binary data to file. */
  def writeBinaryToFile(fileName: String, data: Array[Byte]): Unit =
    useResource(new BufferedOutputStream(new FileOutputStream(fileName))) {
      writer =>
        writer.write(data)
    }


  /** Returns an iterator on the InputStream's data.
    *
    * Closes the stream when read through.
    */
  def readStreamAndClose(is: java.io.InputStream) = new Iterator[Byte] {
    var s = is.read

    def hasNext = s != -1

    def next() = {
      val x = s.toByte; s = is.read;
      if (!hasNext) {
        is.close()
      }; x
    }
  }

  /** Opens a buffered java.io.BufferedInputStream on the file. Closes it after
    * the block is executed.
    */
  def openFileInputStream[T](fileName: File)(func: BufferedInputStream => T) =
    useResource(new BufferedInputStream(new FileInputStream(fileName)))(func)

  /** Merge maps with key collision
    * @param fun
    *   Handles key collision
    */
  def addMaps[K, V](a: Map[K, V], b: Map[K, V])(fun: (V, V) => V): Map[K, V] = {
    a ++ b.map { case (key, bval) =>
      val aval = a.get(key)
      val cval = aval match {
        case None    => bval
        case Some(a) => fun((a), (bval))
      }
      (key, cval)
    }
  }

  /** Merge maps with key collision
    * @param fun
    *   Handles key collision
    */
  def addMaps[K, V](a: collection.Map[K, V], b: collection.Map[K, V])(
      fun: (V, V) => V
  ): collection.Map[K, V] = {
    a ++ b.map { case (key, bval) =>
      val aval = a.get(key)
      val cval = aval match {
        case None    => bval
        case Some(a) => fun((a), (bval))
      }
      (key, cval)
    }
  }

  def retryIO[A](tag: String)(f: => IO[A], c: Int)(implicit
      log: scribe.Logger
  ): IO[A] =
    if (c > 0) f.handleErrorWith { case e =>
      IO.delay(log.error(e, s"Failed $tag. Retry $c more times.")) *>
        IO.sleep(2 seconds) *>
        retryIO(tag)(
          IO.delay(log.debug(s"Retrying $tag")) *> f,
          c - 1
        )
    }
    else f

  def reflectivelyInstantiateObject[A](fqcn: String): A = {
    java.lang.Class
      .forName(fqcn)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[A]
   

  }

  def rightOrThrow[A, E](e: Either[E, A]): A = e match {
    case Right(a)           => a
    case Left(e: Throwable) => throw e
    case Left(e)            => throw new RuntimeException(e.toString)
  }

}
