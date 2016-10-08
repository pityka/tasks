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

import scala.io.Source
import java.io.{PrintWriter, BufferedWriter, FileWriter, FileInputStream, FileOutputStream, BufferedOutputStream, LineNumberReader, InputStream, BufferedReader, FileReader, BufferedInputStream, StringWriter, File, EOFException}
import java.util.zip.GZIPInputStream

import scala.sys.process._
import scala.concurrent._
import scala.concurrent.duration._
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import util.eq._
import scala.collection.mutable.ArrayBuffer
import scala.util._

package object util {

  private def available(port: Int): Boolean = {
    var s: java.net.Socket = null
    try {
      s = new java.net.Socket("localhost", port);

      false
    } catch {
      case e: Exception => true
    } finally {
      if (s != null) {

        s.close

      }
    }
  }

  def chooseNetworkPort: Int =
    Try(config.global.hostPort).flatMap { p =>
      if (available(p)) Success(p) else Failure(new RuntimeException)
    }.getOrElse {

      val s = new java.net.ServerSocket(0);
      val p = s.getLocalPort()
      s.close
      p
    }

  def stackTraceAsString(t: Any): String = {
    if (t.isInstanceOf[Throwable]) {
      val sw = new StringWriter();
      val pw = new PrintWriter(sw);
      t.asInstanceOf[Throwable].printStackTrace(pw);
      sw.toString(); // stack trace as a string
    } else t.toString
  }

  def rethrow[T](messageOnError: => String,
                 exceptionFactory: (=> String,
                                    Throwable) => Throwable)(block: => T): T =
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
      case _ if n > 1 => retry(n - 1)(fn)
      case f => f
    }

  /**
    * Returns the result of the block, and closes the resource.
    *
    * @param param closeable resource
    * @param f block using the resource
    */
  def useResource[A <: { def close(): Unit }, B](param: A)(f: A => B): B =
    try { f(param) } finally {
      param.close()
    }

  /** Writes text data to file. */
  def writeToFile(fileName: String, data: java.lang.String): Unit =
    useResource(new PrintWriter(new BufferedWriter(new FileWriter(fileName)))) {
      writer =>
        writer.write(data)
    }

  /** Writes text data to file. */
  def writeToFile(file: File, data: String): Unit =
    writeToFile(file.getAbsolutePath, data)
  //
  //   /** Writes binary data to file. */
  def writeBinaryToFile(fileName: String, data: Array[Byte]): Unit =
    useResource(new BufferedOutputStream(new FileOutputStream(fileName))) {
      writer =>
        writer.write(data)
    }

  /** Writes binary data to file. */
  def writeBinaryToFile(file: File, data: Array[Byte]): Unit =
    writeBinaryToFile(file.getAbsolutePath, data)

  def writeBinaryToFile(data: Array[Byte]): File = {
    val file = File.createTempFile("tmp", "tmp")
    writeBinaryToFile(file.getAbsolutePath, data)
    file
  }

  /**
    * Returns an iterator on the InputStream's data.
    *
    * Closes the stream when read through.
    */
  def readStreamAndClose(is: java.io.InputStream) = new Iterator[Byte] {
    var s = is.read

    def hasNext = s != -1

    def next = {
      var x = s.toByte; s = is.read; if (!hasNext) { is.close() }; x
    }
  }

  /** Reads file contents into a bytearray. */
  def readBinaryFile(fileName: String): Array[Byte] = {
    useResource(new BufferedInputStream(new FileInputStream(fileName))) { f =>
      readBinaryStream(f)
    }
  }

  /** Reads file contents into a bytearray. */
  def readBinaryFile(f: File): Array[Byte] = readBinaryFile(f.getAbsolutePath)

  /** Reads file contents into a bytearray. */
  def readBinaryStream(f: java.io.InputStream): Array[Byte] = {
    def read(x: List[Byte]): List[Byte] = {
      val raw = f.read
      val ch: Byte = raw.toByte
      if (raw != -1) {
        read(ch :: x)
      } else {
        x
      }
    }
    read(Nil).reverse.toArray
  }

  /** Opens a buffered [[java.io.BufferedOutputStream]] on the file. Closes it after the block is executed. */
  def openFileOutputStream[T](fileName: File, append: Boolean = false)(
      func: BufferedOutputStream => T) =
    useResource(
        new BufferedOutputStream(new FileOutputStream(fileName, append)))(func)

  /** Opens a buffered [[java.io.BufferedInputStream]] on the file. Closes it after the block is executed. */
  def openFileInputStream[T](fileName: File)(func: BufferedInputStream => T) =
    useResource(new BufferedInputStream(new FileInputStream(fileName)))(func)

  /**
    * Execute command with user function to process each line of output.
    *
    * Based on from http://www.jroller.com/thebugslayer/entry/executing_external_system_commands_in
    * Creates 3 new threads: one for the stdout, one for the stderror, and one waits for the exit code.
    * @param pb Description of the executable process
    * @param atMost Maximum time to wait for the process to complete. Default infinite.
    * @return Exit code of the process.
    */
  def exec(pb: ProcessBuilder,
           atMost: Duration = Duration.Inf)(stdOutFunc: String => Unit = {
    x: String =>
    })(implicit stdErrFunc: String => Unit = (x: String) => ()): Int = {

    import java.util.concurrent.Executors

    val executorService = Executors.newSingleThreadExecutor

    implicit val ec = ExecutionContext.fromExecutorService(executorService)

    val process = pb.run(ProcessLogger(stdOutFunc, stdErrFunc))

    val hook = try {
      scala.sys.addShutdownHook { process.destroy() }
    } catch {
      case x: Throwable => {
        Try(process.destroy)
        Try(executorService.shutdownNow)
        throw x
      }
    }

    try {
      val f = Future { process.exitValue }
      Await.result(f, atMost = atMost)
    } finally {
      Try(process.destroy)
      Try(executorService.shutdownNow)
      Try(hook.remove)

    }
  }

  /**
    * Execute command. Returns stdout and stderr as strings, and true if it was successful.
    *
    * A process is considered successful if its exit code is 0 and the error stream is empty.
    * The latter criterion can be disabled with the unsuccessfulOnErrorStream parameter.
    * @param pb The process description.
    * @param unsuccessfulOnErrorStream if true, then the process is considered as a failure if its stderr is not empty.
    * @param atMost max waiting time.
    * @return (stdout,stderr,success) triples
    */
  def execGetStreamsAndCode(pb: ProcessBuilder,
                            unsuccessfulOnErrorStream: Boolean = true,
                            atMost: Duration = Duration.Inf)
    : (List[String], List[String], Boolean) = {
    var ls: List[String] = Nil
    var lse: List[String] = Nil
    var boolean = true
    val exitvalue = exec(pb, atMost) { ln =>
      ls = ln :: ls
    } { ln =>
      if (unsuccessfulOnErrorStream) { boolean = false }; lse = ln :: lse
    }
    (ls.reverse, lse.reverse, boolean && (exitvalue == 0))
  }

  /**
    * Execute command. Returns stdout and stderr as strings, and true if it was successful. Also writes to log.
    *
    * A process is considered successful if its exit code is 0 and the error stream is empty.
    * The latter criterion can be disabled with the unsuccessfulOnErrorStream parameter.
    * @param pb The process description.
    * @param unsuccessfulOnErrorStream if true, then the process is considered as a failure if its stderr is not empty.
    * @param atMost max waiting time.
    * @param log A logger.
    * @return (stdout,stderr,success) triples
    */
  def execGetStreamsAndCodeWithLog(
      pb: ProcessBuilder,
      unsuccessfulOnErrorStream: Boolean = true,
      atMost: Duration = Duration.Inf)(implicit log: {
    def info(s: String): Unit; def error(s: String): Unit
  }): (List[String], List[String], Boolean) = {
    var ls: List[String] = Nil
    var lse: List[String] = Nil
    var boolean = true
    val exitvalue = exec(pb, atMost) { ln =>
      ls = ln :: ls; log.info(ln)
    } { ln =>
      if (unsuccessfulOnErrorStream) { boolean = false }; lse = ln :: lse;
      if (unsuccessfulOnErrorStream) log.error(ln) else log.info(ln)
    }
    (ls.reverse, lse.reverse, boolean && (exitvalue == 0))
  }

  /**
    * Merge maps with key collision
    * @param fun Handles key collision
    */
  def addMaps[K, V](a: Map[K, V], b: Map[K, V])(fun: (V, V) => V): Map[K, V] = {
    a ++ b.map {
      case (key, bval) =>
        val aval = a.get(key)
        val cval = aval match {
          case None => bval
          case Some(a) => fun((a), (bval))
        }
        (key, cval)
    }
  }

  /**
    * Merge maps with key collision
    * @param fun Handles key collision
    */
  def addMaps[K, V](a: collection.Map[K, V], b: collection.Map[K, V])(
      fun: (V, V) => V): collection.Map[K, V] = {
    a ++ b.map {
      case (key, bval) =>
        val aval = a.get(key)
        val cval = aval match {
          case None => bval
          case Some(a) => fun((a), (bval))
        }
        (key, cval)
    }
  }

}
