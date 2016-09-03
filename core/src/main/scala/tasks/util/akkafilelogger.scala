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

package tasks.util

import akka.event.Logging.InitializeLogger
import akka.event.Logging.LoggerInitialized
import akka.event.Logging.Error
import akka.event.Logging.Warning
import akka.event.Logging.Info
import akka.event.Logging.Debug
import akka.actor.{Actor}
import akka.event.Logging.LogEvent
import scala.util.control.NoStackTrace

import tasks.shared._

class FileLogger(file: java.io.File, filter: Option[String]) extends Actor {

  def this(file: java.io.File) = this(file, None)

  import java.text.SimpleDateFormat
  import java.util.Date

  val writer = new java.io.FileWriter(file)

  override def postStop {
    writer.close
  }

  private def stackTraceFor(e: Throwable): String = e match {
    case null | Error.NoCause ⇒ ""
    case _: NoStackTrace ⇒ " (" + e.getClass.getName + ")"
    case other ⇒
      val sw = new java.io.StringWriter
      val pw = new java.io.PrintWriter(sw)
      pw.append('\n')
      other.printStackTrace(pw)
      sw.toString
  }

  private val date = new Date()
  private val dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS")
  private val errorFormat = "[ERROR] [%s] [%s] [%s] %s%s\n".intern
  private val errorFormatWithoutCause = "[ERROR] [%s] [%s] [%s] %s\n".intern
  private val warningFormat = "[WARN] [%s] [%s] [%s] %s\n".intern
  private val infoFormat = "[INFO] [%s] [%s] [%s] %s\n".intern
  private val debugFormat = "[DEBUG] [%s] [%s] [%s] %s\n".intern

  def timestamp: String = synchronized { dateFormat.format(new Date) }

  def print(event: Any): Unit = event match {
    case e: Error => error(e)
    case e: Warning => warning(e)
    case e: Info => info(e)
    case e: Debug => debug(e)
    case e => other(e)
  }

  def other(e: Any): Unit = writer.write(e + "\n")

  def error(event: Error): Unit = {
    val f =
      if (event.cause == Error.NoCause) errorFormatWithoutCause
      else errorFormat
    writer.write(
        f.format(
            timestamp,
            event.thread.getName,
            event.logSource,
            event.message,
            stackTraceFor(event.cause)
        ))
  }

  def warning(event: Warning): Unit =
    writer.write(
        warningFormat.format(
            timestamp,
            event.thread.getName,
            event.logSource,
            event.message
        ))

  def info(event: Info): Unit =
    writer.write(
        infoFormat.format(
            timestamp,
            event.thread.getName,
            event.logSource,
            event.message
        ))

  def debug(event: Debug): Unit =
    writer.write(
        debugFormat.format(
            timestamp,
            event.thread.getName,
            event.logSource,
            event.message
        ))

  def receive = {
    case InitializeLogger(_) => sender ! LoggerInitialized
    case x: LogEvent
        if filter.map(f => x.logSource.startsWith(f)).getOrElse(true) => {
      print(x)
      writer.flush
    }
    case _ => {}
  }
}

class LogPublishActor(filter: Option[String]) extends Actor {

  import java.text.SimpleDateFormat
  import java.util.Date

  private def stackTraceFor(e: Throwable): String = e match {
    case null | Error.NoCause ⇒ ""
    case _: NoStackTrace ⇒ " (" + e.getClass.getName + ")"
    case other ⇒
      val sw = new java.io.StringWriter
      val pw = new java.io.PrintWriter(sw)
      pw.append('\n')
      other.printStackTrace(pw)
      sw.toString
  }

  private val dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS")
  def receive = {
    case InitializeLogger(_) => sender ! LoggerInitialized
    case x: Error
        if filter.map(f => x.logSource.startsWith(f)).getOrElse(true) =>
      context.system.eventStream.publish(
          LogMessage("ERROR",
                     x.message.toString,
                     dateFormat.format(new Date),
                     stackTraceFor(x.cause)))
    case x: Warning
        if filter.map(f => x.logSource.startsWith(f)).getOrElse(true) =>
      context.system.eventStream.publish(
          LogMessage("WARNING",
                     x.message.toString,
                     dateFormat.format(new Date),
                     ""))
    case x: Info
        if filter.map(f => x.logSource.startsWith(f)).getOrElse(true) => {
      context.system.eventStream.publish(
          LogMessage("INFO",
                     x.message.toString,
                     dateFormat.format(new Date),
                     ""))
    }

    case x => {}
  }
}
