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

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import scala.concurrent.duration._

package object config {

  val global = ConfigFactory.load()

  // Everything in seconds
  val ProxyTaskGetBackResult = global.getInt("tasks.proxytaskGetBackResultTimeoutInSeconds") //60 * 60 * 168 * 4
  val PingTimeout = 60
  val LauncherActorHeartBeatInterval = Duration(global.getMilliseconds("tasks.failuredetector.heartbeat-interval"), MILLISECONDS) // seconds
  val WaitForSaveTimeOut = 60 * 2

  val fileSendChunkSize = global.getBytes("tasks.fileSendChunkSize").toInt

  val includeFullPathInDefaultSharedName = global.getBoolean("tasks.includeFullPathInDefaultSharedName")

  val resubmitFailedTask = global.getBoolean("tasks.resubmitFailedTask")

  val logToStandardOutput = global.getBoolean("tasks.stdout")

  val verifySharedFileInCache = global.getBoolean("tasks.verifySharedFileInCache")

  val disableRemoting = global.getBoolean("tasks.disableRemoting")

  val nonLocalFileSystems = global.getStringList("tasks.nonLocalFileSystems").map(f => new java.io.File(f))

  val skipContentHashVerificationAfterCache = global.getBoolean("tasks.skipContentHashVerificationAfterCache")

  val AcceptableHeartbeatPause = Duration(global.getMilliseconds("tasks.failuredetector.acceptable-heartbeat-pause"), MILLISECONDS)

}
